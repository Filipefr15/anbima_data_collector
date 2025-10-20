package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"

	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
)

func conectaDB() (*sql.DB, error) {
	err := godotenv.Load(".env")
	if err != nil {
		return nil, fmt.Errorf("erro ao carregar arquivo .env: %v", err)
	}

	host := os.Getenv("HOST")
	port := os.Getenv("PORT")
	user := os.Getenv("USER")
	password := os.Getenv("PASSWORD")
	dbname := os.Getenv("DATABASE")

	connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable", host, port, user, password, dbname)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("erro ao abrir conexão com banco de dados: %v", err)
	}

	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("erro ao conectar com banco de dados: %v", err)
	}

	return db, nil
}

// executarConsulta executa uma consulta SQL genérica a partir de um arquivo
func executarConsulta(db *sql.DB, arquivoSQL string) (*sql.Rows, error) {
	// Lê a consulta SQL do arquivo
	sqlBytes, err := ioutil.ReadFile(arquivoSQL)
	if err != nil {
		return nil, fmt.Errorf("erro ao ler arquivo SQL: %v", err)
	}

	query := strings.TrimSpace(string(sqlBytes))

	// Executa a consulta
	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("erro ao executar consulta: %v", err)
	}

	return rows, nil
}

func executarConsultaWithOneParam(db *sql.DB, arquivoSQL string, varName, param string) (*sql.Rows, error) {
	// Lê a consulta SQL do arquivo
	sqlBytes, err := ioutil.ReadFile(arquivoSQL)
	if err != nil {
		return nil, fmt.Errorf("erro ao ler arquivo SQL: %v", err)
	}

	varName = ":" + varName

	query := strings.TrimSpace(string(sqlBytes))
	query = strings.Replace(query, varName, param, 1)

	// Executa a consulta
	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("erro ao executar consulta: %v", err)
	}

	return rows, nil
}

func executarConsultaWithParams(db *sql.DB, arquivoSQL string, params map[string]string, noParams bool) (*sql.Rows, error) {
	// Lê a consulta SQL do arquivo
	sqlBytes, err := ioutil.ReadFile(arquivoSQL)
	if err != nil {
		return nil, fmt.Errorf("erro ao ler arquivo SQL: %v", err)
	}

	query := strings.TrimSpace(string(sqlBytes))
	if noParams {
		query += " WHERE "
		for key, value := range params {
			if value != "" {
				query += fmt.Sprintf(" %s = '%s' AND", key, value)
			}
		}
		query = strings.TrimSuffix(query, " AND")
	}

	// Executa a consulta
	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("erro ao executar consulta: %v", err)
	}

	return rows, nil
}

func database(tableName, csvFile string) {

	godotenv.Load(".env")

	host := os.Getenv("HOST")
	port := os.Getenv("PORT")
	user := os.Getenv("USER")
	password := os.Getenv("PASSWORD")
	dbName := os.Getenv("DATABASE")
	// Configuração inicial (conecta ao banco postgres padrão)
	//adminConnStr := "host=localhost port=5432 user=postgres password=123456 dbname=postgres sslmode=disable"
	// dbName := "anbimainfo"
	// tableName := "cadastro_adm_fii"
	// csvFile := "adm_fii_padronized/cad_adm_fii.csv"

	connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable", host, port, user, password, dbName)
	// 1. Cria o banco de dados se não existir
	if err := createDatabase(connStr, dbName); err != nil {
		log.Fatal(err)
	}

	// 2. Conecta ao banco criado
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatal(err)
	}

	// // 3. Cria a tabela baseada no CSV
	if err := createTableFromCSV(db, csvFile, tableName); err != nil {
		log.Fatal(err)
	}

	// 4. Importa os dados
	if err := importCSV(db, csvFile, tableName); err != nil {
		log.Fatal(err)
	}

	fmt.Println("✓ Banco, tabela e dados criados com sucesso!")
}

// createDatabase cria o banco de dados se não existir
func createDatabase(connStr, dbName string) error {
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return err
	}
	defer db.Close()

	// Verifica se o banco existe
	var exists bool
	query := "SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1)"
	err = db.QueryRow(query, dbName).Scan(&exists)
	if err != nil {
		return err
	}

	if !exists {
		_, err = db.Exec(fmt.Sprintf("CREATE DATABASE %s", dbName))
		if err != nil {
			return fmt.Errorf("erro ao criar banco: %w", err)
		}
		fmt.Printf("✓ Banco '%s' criado\n", dbName)
	} else {
		fmt.Printf("✓ Banco '%s' já existe\n", dbName)
	}

	return nil
}

// createTableFromCSV cria a tabela baseada no cabeçalho do CSV
func createTableFromCSV(db *sql.DB, csvFile, tableName string) error {
	f, err := os.Open(csvFile)
	if err != nil {
		return fmt.Errorf("erro ao abrir CSV: %w", err)
	}
	defer f.Close()

	reader := csv.NewReader(f)
	reader.Comma = ',' // Ajuste conforme necessário

	// Lê o cabeçalho
	header, err := reader.Read()
	if err != nil {
		return fmt.Errorf("erro ao ler cabeçalho: %w", err)
	}

	// Lê algumas linhas para inferir tipos
	sampleRows := [][]string{}
	for i := 0; i < 50; i++ {
		row, err := reader.Read()
		if err != nil {
			break
		}
		sampleRows = append(sampleRows, row)
	}

	// Dropa a tabela se existir
	// _, err = db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
	// if err != nil {
	// 	return err
	// }

	// Constrói o CREATE TABLE
	var columns []string
	for i, colName := range header {
		// Limpa o nome da coluna
		colName = cleanColumnName(colName)
		// Infere o tipo de dado
		colType := inferType(sampleRows, i, header)
		columns = append(columns, fmt.Sprintf("%s %s", colName, colType))
	}

	// createSQL := fmt.Sprintf("CREATE TABLE %s (\n  id SERIAL PRIMARY KEY,\n  %s\n)",
	createSQL := fmt.Sprintf("CREATE TABLE %s (%s\n)",
		tableName,
		strings.Join(columns, ",\n  "))

	_, err = db.Exec(createSQL)
	if err != nil {
		fmt.Println("erro ao criar tabela: %w - continuando mesmo assim", err)
	}

	fmt.Printf("✓ Tabela '%s' criada com %d colunas\n", tableName, len(header))
	fmt.Println("\nEstrutura da tabela:")
	fmt.Println(createSQL)
	fmt.Println()

	return nil
}

// cleanColumnName limpa e formata o nome da coluna
func cleanColumnName(name string) string {
	name = strings.TrimSpace(name)
	name = strings.ToLower(name)
	name = strings.ReplaceAll(name, " ", "_")
	name = strings.ReplaceAll(name, "-", "_")
	// Remove caracteres especiais
	var result strings.Builder
	for _, r := range name {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_' {
			result.WriteRune(r)
		}
	}
	return result.String()
}

// inferType tenta inferir o tipo de dado da coluna
func inferType(rows [][]string, colIndex int, header []string) string {
	if len(rows) == 0 {
		return "TEXT"
	}

	// Verifica se o nome da coluna começa com "cnpj"
	if colIndex < len(header) && strings.HasPrefix(strings.ToLower(header[colIndex]), "cnpj") {
		return "TEXT"
	}

	isInt := true
	isFloat := true
	isDate := true
	hasNonNumericChars := false
	validValuesCount := 0

	for _, row := range rows {
		if colIndex >= len(row) {
			continue
		}
		val := strings.TrimSpace(row[colIndex])
		valLower := strings.ToLower(val)

		// Ignora valores vazios/nulos mas continua verificando
		if val == "" || valLower == "null" || valLower == "na" || valLower == "nan" || valLower == "n/a" {
			continue
		}

		validValuesCount++

		// Verifica se é data no formato AAAA-MM-DD
		if len(val) == 10 && val[4] == '-' && val[7] == '-' {
			parts := strings.Split(val, "-")
			if len(parts) == 3 {
				var y, m, d int
				if _, err := fmt.Sscanf(parts[0], "%d", &y); err != nil || y < 1900 || y > 2100 {
					isDate = false
				}
				if _, err := fmt.Sscanf(parts[1], "%d", &m); err != nil || m < 1 || m > 12 {
					isDate = false
				}
				if _, err := fmt.Sscanf(parts[2], "%d", &d); err != nil || d < 1 || d > 31 {
					isDate = false
				}
			} else {
				isDate = false
			}
		} else {
			isDate = false
		}

		if strings.Contains(val, "/") ||
			strings.Count(val, ".") > 1 ||
			strings.Count(val, "-") > 1 {
			hasNonNumericChars = true
		}
		for _, r := range val {
			if r != '-' && r != '.' && r != ',' && (r < '0' || r > '9') {
				hasNonNumericChars = true
				break
			}
		}

		if strings.Contains(val, ".") || strings.Contains(val, ",") {
			isInt = false
		} else {
			testVal := strings.TrimPrefix(val, "-")
			for _, r := range testVal {
				if r < '0' || r > '9' {
					isInt = false
					break
				}
			}
			if isInt && len(testVal) > 0 {
				const maxInt32 = 2147483647
				const minInt32 = -2147483648
				var num int64
				if _, err := fmt.Sscanf(val, "%d", &num); err == nil {
					if num > maxInt32 || num < minInt32 {
						isInt = false
					}
				} else {
					isInt = false
				}
			}
		}
		if !hasNonNumericChars {
			var f float64
			testVal := strings.ReplaceAll(val, ",", ".")
			if _, err := fmt.Sscanf(testVal, "%f", &f); err != nil {
				isFloat = false
			}
		} else {
			isFloat = false
		}
	}

	if validValuesCount == 0 {
		return "TEXT"
	}
	if isDate {
		return "DATE"
	}
	if hasNonNumericChars {
		return "TEXT"
	}
	if isInt {
		return "INTEGER"
	}
	if isFloat {
		return "DECIMAL"
	}
	return "TEXT"
}

// formataCNPJ recebe um valor e retorna o CNPJ formatado (se possível)
func formataCNPJ(val string) string {
	// Remove tudo que não é dígito
	var digits strings.Builder
	for _, r := range val {
		if r >= '0' && r <= '9' {
			digits.WriteRune(r)
		}
	}
	cnpj := digits.String()
	if len(cnpj) != 14 && strings.ToLower(cnpj) == "nan" {
		return val // Não é um CNPJ válido, retorna original
	} else {
		zerosNeeded := 14 - len(cnpj)
		if zerosNeeded > 0 {
			cnpj = strings.Repeat("0", zerosNeeded) + cnpj
		}
	}
	// Formata: 00.000.000/0000-00
	return fmt.Sprintf("%s.%s.%s/%s-%s",
		cnpj[0:2], cnpj[2:5], cnpj[5:8], cnpj[8:12], cnpj[12:14])
}

// importCSV importa os dados usando COPY FROM
func importCSV(db *sql.DB, csvFile, tableName string) error {
	f, err := os.Open(csvFile)
	if err != nil {
		return err
	}
	defer f.Close()

	reader := csv.NewReader(f)
	reader.Comma = ','
	reader.LazyQuotes = true

	// Lê o cabeçalho
	header, err := reader.Read()
	if err != nil {
		return err
	}

	// Limpa os nomes das colunas
	for i := range header {
		header[i] = cleanColumnName(header[i])
	}

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Usa INSERT em lotes (mais compatível que COPY)
	batch := [][]string{}
	batchSize := 1000
	recordCount := 0

	for {
		record, err := reader.Read()
		if err != nil {
			// Insere último lote
			if len(batch) > 0 {
				if err := insertBatch(tx, tableName, header, batch); err != nil {
					return err
				}
				recordCount += len(batch)
			}
			break
		}

		batch = append(batch, record)

		if len(batch) >= batchSize {
			if err := insertBatch(tx, tableName, header, batch); err != nil {
				return err
			}
			recordCount += len(batch)
			fmt.Printf("\r✓ Importados %d registros...", recordCount)
			batch = batch[:0]
		}
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	fmt.Printf("\r✓ Importados %d registros no total\n", recordCount)
	return nil
}

func insertBatch(tx *sql.Tx, tableName string, header []string, batch [][]string) error {
	if len(batch) == 0 {
		return nil
	}

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES ",
		tableName,
		strings.Join(header, ", "))

	var values []interface{}
	for i, record := range batch {
		if i > 0 {
			query += ", "
		}
		query += "("
		for j, val := range record {
			if j > 0 {
				query += ", "
			}
			query += fmt.Sprintf("$%d", len(values)+1)
			valTrimmed := strings.TrimSpace(val)
			valLower := strings.ToLower(valTrimmed)
			// Verifica se a coluna é CNPJ
			if strings.HasPrefix(header[j], "cnpj") && valTrimmed != "" {
				valTrimmed = formataCNPJ(valTrimmed)
			}
			// Trata valores vazios e nulos como NULL
			if valTrimmed == "" || valLower == "null" || valLower == "na" || valLower == "nan" || valLower == "n/a" {
				values = append(values, nil)
			} else {
				values = append(values, valTrimmed)
			}
		}
		query += ")"
	}

	_, err := tx.Exec(query, values...)
	return err
}
