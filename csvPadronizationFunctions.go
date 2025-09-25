package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"
	"golang.org/x/text/encoding/charmap"
	"golang.org/x/text/transform"
)

// prefix para fidc => fidc
// func especifica para fidc.
func csvPadronizationFidc(tabs []string, auxs, meses []int, prefix string, mapColNameValue, mapColumnToTransform map[string]string) error {
	for _, tab := range tabs {
		for _, aux := range auxs {
			var merged dataframe.DataFrame
			first := true

			dfCh := make(chan dataframe.DataFrame)
			var wg sync.WaitGroup

			for _, mes := range meses {
				arquivo := fmt.Sprintf("fidc/inf_mensal_fidc_tab%s%d%02d.csv", tab, aux, mes)
				if _, err := os.Stat(arquivo); err != nil {
					continue
				}

				wg.Add(1)
				go func(arquivo string) {
					defer wg.Done()
					err := addColumnToCsvFile(arquivo, mapColNameValue, mapColumnToTransform)
					if err != nil {
						fmt.Printf("Erro ao adicionar coluna em %s: %v\n", arquivo, err)
					}
					f, err := os.Open(arquivo)
					if err != nil {
						fmt.Printf("Erro ao abrir arquivo %s: %v\n", arquivo, err)
						return
					}
					defer f.Close()

					reader := transform.NewReader(f, charmap.ISO8859_1.NewDecoder())
					scanner := bufio.NewScanner(reader)

					var records [][]string
					var header []string
					lineNum := 0

					for scanner.Scan() {
						lineNum++
						line := scanner.Text()

						// Processa a linha como CSV
						r := csv.NewReader(strings.NewReader(line))
						r.Comma = ';'
						r.LazyQuotes = true

						row, err := r.Read()
						if err != nil {
							fmt.Printf("Erro ao ler linha %d em %s: %v\n", lineNum, arquivo, err)

							// Tenta reparar a linha usando a função tryFixCsvLine
							if header != nil { // só tenta reparar se já temos o header
								fixedRow := fixCsvLine(arquivo, lineNum, len(header))
								if fixedRow != nil {
									records = append(records, fixedRow)
									continue
								}
							}
							// Se não conseguiu reparar, pula esta linha
							fmt.Printf("Pulando linha %d irrecuperável\n", lineNum)
							continue
						}

						// Limpa aspas soltas
						for i, val := range row {
							row[i] = strings.TrimPrefix(val, `"`)
							row[i] = strings.TrimSuffix(val, `"`)
						}

						if header == nil {
							header = row
							records = append(records, row)
							continue
						}

						// Ajusta linhas com excesso de campos automaticamente
						if len(row) != len(header) {
							fmt.Printf("Linha %d tem %d campos, header tem %d campos\n", lineNum, len(row), len(header))

							// Tenta reparar usando tryFixCsvLine primeiro
							fixedRow := fixCsvLine(arquivo, lineNum, len(header))
							if fixedRow != nil {
								records = append(records, fixedRow)
								continue
							}

							// Se tryFixCsvLine não funcionou, tenta o método automático
							diff := len(row) - len(header)
							if diff > 0 {
								// Junta os campos extras no campo que provavelmente contém texto (como nome do fundo)
								// Procuramos o primeiro campo que seja "não numérico" como candidato
								for i, val := range row {
									if _, err := fmt.Sscanf(val, "%f", new(float64)); err != nil {
										// Found candidate for merge
										mergedVal := strings.Join(row[i:i+diff+1], ";")
										newRow := append(row[:i], mergedVal)
										if i+diff+1 < len(row) {
											newRow = append(newRow, row[i+diff+1:]...)
										}
										row = newRow
										break
									}
								}
							}
						}

						// Ignora linhas que ainda não batem
						if len(row) != len(header) {
							fmt.Printf("Ignorando linha irrecuperável %d em %s: %v\n", lineNum, arquivo, row)
							continue
						}

						records = append(records, row)
					}

					if err := scanner.Err(); err != nil {
						fmt.Printf("Erro ao escanear arquivo %s: %v\n", arquivo, err)
					}

					if len(records) > 0 {
						df := dataframe.LoadRecords(records)

						dfCh <- df
					}
				}(arquivo)
			}

			// Fechar channel após todas as goroutines terminarem
			go func() {
				wg.Wait()
				close(dfCh)
			}()

			// Coleta os dataframes e faz RBind
			for df := range dfCh {
				if first {
					merged = df
					first = false
				} else {
					merged = merged.RBind(df)
				}
			}

			if merged.Nrow() == 0 {
				fmt.Printf("Nenhum dado encontrado para %s no ano %d, mes %d\n", tab, aux, meses)
				continue
			}
			os.MkdirAll(fmt.Sprintf("%s_padronized", prefix), os.ModePerm)

			outFileName := fmt.Sprintf("%s_padronized/fidc_mensal_%d%s.csv", prefix, aux, tab)
			outFile, err := os.Create(outFileName)
			if err != nil {
				return err
			}
			if err := merged.WriteCSV(outFile); err != nil {
				return err
			}
			outFile.Close()
			fmt.Printf("Arquivo %s gerado com sucesso!\n", outFileName)
		}
	}

	return nil
}

// PADRAO PARA REPASSAR PARA TODAS AS OUTRAS!!!
func csvPadronizationLamina(tabs []string, anos, meses []int) error {
	const maxGoroutines = 15
	sem := make(chan struct{}, maxGoroutines)

	for _, tab := range tabs {
		for _, ano := range anos {
			// dfCh := make(chan dataframe.DataFrame)
			var wg sync.WaitGroup

			for _, mes := range meses {
				arquivo := fmt.Sprintf("lamina/lamina_fi%s%d%02d.csv", tab, ano, mes)
				if _, err := os.Stat(arquivo); err != nil {
					continue
				}

				sem <- struct{}{}
				wg.Add(1)
				go func(arquivo string) {
					defer wg.Done()
					defer func() { <-sem }()

					f, err := os.Open(arquivo)
					if err != nil {
						fmt.Printf("Erro ao abrir arquivo %s: %v\n", arquivo, err)
						return
					}
					defer f.Close()

					reader := transform.NewReader(f, charmap.ISO8859_1.NewDecoder())
					scanner := bufio.NewScanner(reader)

					var records [][]string
					var header []string
					lineNum := 0

					for scanner.Scan() {
						lineNum++
						line := scanner.Text()

						r := csv.NewReader(strings.NewReader(line))
						r.Comma = ';'
						r.LazyQuotes = true

						row, err := r.Read()
						if err != nil {
							fmt.Printf("Erro ao ler linha %d em %s: %v\n", lineNum, arquivo, err)
							if header != nil {
								fixedRow := fixCsvLine(arquivo, lineNum, len(header))
								if fixedRow != nil {
									records = append(records, fixedRow)
									continue
								}
							}
							fmt.Printf("Pulando linha %d irrecuperável\n", lineNum)
							continue
						}

						// for i, val := range row {
						// 	row[i] = strings.TrimPrefix(val, `"`)
						// 	row[i] = strings.TrimSuffix(val, `"`)
						// }

						if header == nil {
							header = row
							records = append(records, row)
							continue
						}

						if len(row) != len(header) {
							fmt.Printf("Linha %d tem %d campos, header tem %d campos\n", lineNum, len(row), len(header))
							fixedRow := fixCsvLine(arquivo, lineNum, len(header))
							if fixedRow != nil {
								records = append(records, fixedRow)
								continue
							}
							diff := len(row) - len(header)
							if diff > 0 {
								for i, val := range row {
									if _, err := fmt.Sscanf(val, "%f", new(float64)); err != nil {
										mergedVal := strings.Join(row[i:i+diff+1], ";")
										newRow := append(row[:i], mergedVal)
										if i+diff+1 < len(row) {
											newRow = append(newRow, row[i+diff+1:]...)
										}
										row = newRow
										break
									}
								}
							}
						}

						if len(row) != len(header) {
							fmt.Printf("Ignorando linha irrecuperável %d em %s: %v\n", lineNum, arquivo, row)
							continue
						}

						records = append(records, row)
					}

					if err := scanner.Err(); err != nil {
						fmt.Printf("Erro ao escanear arquivo %s: %v\n", arquivo, err)
					}

					if len(records) > 0 {
						df := dataframe.LoadRecords(records)

						// colMap := map[string]string{
						// 	"CNPJ_FUNDO": "CNPJ_FUNDO_CLASSE",
						// }
						// for _, colName := range df.Names() {
						// 	if newName, ok := colMap[colName]; ok && newName != colName {
						// 		df = df.Rename(newName, colName)
						// 	}
						// }

						colMap := map[string]string{
							"TP_FUNDO":   "TP_FUNDO_CLASSE",
							"CNPJ_FUNDO": "CNPJ_FUNDO_CLASSE",
						}
						for _, colName := range df.Names() {
							if newName, ok := colMap[colName]; ok && newName != colName {
								df = df.Rename(newName, colName)
							}
						}

						mapColNameValue := map[string]string{
							"TP_FUNDO_CLASSE": "Não informado",
							"ID_SUBCLASSE":    "",
						}

						for colName, colValue := range mapColNameValue {
							vals := make([]string, df.Nrow())
							for i := range vals {
								vals[i] = colValue
							}
							newCol := series.New(vals, series.String, colName)
							df = df.Mutate(newCol)
						}

						os.MkdirAll("lamina_padronized", os.ModePerm)
						outFileName := fmt.Sprintf("lamina_padronized/lamina_fi%s%d%02d.csv", tab, ano, mes)
						outFile, err := os.Create(outFileName)
						if err != nil {
							fmt.Printf("Erro ao criar arquivo %s: %v\n", outFileName, err)
						}
						if err := df.WriteCSV(outFile); err != nil {
							fmt.Printf("Erro ao escrever CSV em %s: %v\n", outFileName, err)
						}
						outFile.Close()
						fmt.Printf("Arquivo %s gerado com sucesso!\n", outFileName)
					}
				}(arquivo)
			}
			wg.Wait()
		}
	}

	return nil
}

// tab := []string{"adm_fii"} cadOuDoc := "cad"
// padroniza os CSV simples, que precisam apenas de padronização de colunas
// e verificação de colunas
// aux := _classe, _fundo, _subclasse// prefix = registro_
func simpleCsvPadronization(tabs, auxs []string, cadOuDoc, prefix string) error {
	for _, tab := range tabs {
		for _, aux := range auxs {
			var merged dataframe.DataFrame
			first := true

			dfCh := make(chan dataframe.DataFrame)
			var wg sync.WaitGroup

			arquivo := ""
			if prefix != "" {
				arquivo = fmt.Sprintf("%s/%s_%s.csv", tab, prefix, aux)
				if _, err := os.Stat(arquivo); err != nil {
					continue
				}
			} else {
				arquivo = fmt.Sprintf("%s/%s_%s.csv", tab, cadOuDoc, tab)
				if _, err := os.Stat(arquivo); err != nil {
					continue
				}
			}

			wg.Add(1)
			go func(arquivo string) {
				defer wg.Done()

				f, err := os.Open(arquivo)
				if err != nil {
					fmt.Printf("Erro ao abrir arquivo %s: %v\n", arquivo, err)
					return
				}
				defer f.Close()

				reader := transform.NewReader(f, charmap.ISO8859_1.NewDecoder())
				scanner := bufio.NewScanner(reader)

				var records [][]string
				var header []string
				lineNum := 0

				for scanner.Scan() {
					lineNum++
					line := scanner.Text()

					// Processa a linha como CSV
					r := csv.NewReader(strings.NewReader(line))
					r.Comma = ';'
					r.LazyQuotes = true

					row, err := r.Read()
					if err != nil {
						fmt.Printf("Erro ao ler linha %d em %s: %v\n", lineNum, arquivo, err)

						// Tenta reparar a linha usando a função tryFixCsvLine
						if header != nil { // só tenta reparar se já temos o header
							fixedRow := fixCsvLine(arquivo, lineNum, len(header))
							if fixedRow != nil {
								records = append(records, fixedRow)
								continue
							}
						}
						// Se não conseguiu reparar, pula esta linha
						fmt.Printf("Pulando linha %d irrecuperável\n", lineNum)
						continue
					}

					// Limpa aspas soltas
					for i, val := range row {
						row[i] = strings.TrimPrefix(val, `"`)
						row[i] = strings.TrimSuffix(val, `"`)
					}

					if header == nil {
						header = row
						records = append(records, row)
						continue
					}

					// Ajusta linhas com excesso de campos automaticamente
					if len(row) != len(header) {
						fmt.Printf("Linha %d tem %d campos, header tem %d campos\n", lineNum, len(row), len(header))

						// Tenta reparar usando tryFixCsvLine primeiro
						fixedRow := fixCsvLine(arquivo, lineNum, len(header))
						if fixedRow != nil {
							records = append(records, fixedRow)
							continue
						}

						// Se tryFixCsvLine não funcionou, tenta o método automático
						diff := len(row) - len(header)
						if diff > 0 {
							// Junta os campos extras no campo que provavelmente contém texto (como nome do fundo)
							// Procuramos o primeiro campo que seja "não numérico" como candidato
							for i, val := range row {
								if _, err := fmt.Sscanf(val, "%f", new(float64)); err != nil {
									// Found candidate for merge
									mergedVal := strings.Join(row[i:i+diff+1], ";")
									newRow := append(row[:i], mergedVal)
									if i+diff+1 < len(row) {
										newRow = append(newRow, row[i+diff+1:]...)
									}
									row = newRow
									break
								}
							}
						}
					}

					// Ignora linhas que ainda não batem
					if len(row) != len(header) {
						fmt.Printf("Ignorando linha irrecuperável %d em %s: %v\n", lineNum, arquivo, row)
						continue
					}

					records = append(records, row)
				}

				if err := scanner.Err(); err != nil {
					fmt.Printf("Erro ao escanear arquivo %s: %v\n", arquivo, err)
				}

				if len(records) > 0 {
					df := dataframe.LoadRecords(records)

					// Padroniza colunas ANTES do merge
					colMap := map[string]string{
						"CNPJ_FUNDO": "CNPJ_FUNDO_CLASSE",
					}
					for _, colName := range df.Names() {
						if newName, ok := colMap[colName]; ok && newName != colName {
							df = df.Rename(newName, colName)
						}
					}

					colMap = map[string]string{
						"TP_FUNDO": "TP_FUNDO_CLASSE",
					}
					for _, colName := range df.Names() {
						if newName, ok := colMap[colName]; ok && newName != colName {
							df = df.Rename(newName, colName)
						}
					}

					dfCh <- df
				}
			}(arquivo)

			// Fechar channel após todas as goroutines terminarem
			go func() {
				wg.Wait()
				close(dfCh)
			}()

			// Coleta os dataframes e faz RBind
			for df := range dfCh {
				if first {
					merged = df
					first = false
				} else {
					merged = merged.RBind(df)
				}
			}

			if merged.Nrow() == 0 {
				fmt.Printf("Nenhum dado encontrado para o ano %s\n", tabs)
				continue
			}
			os.MkdirAll(fmt.Sprintf("%s_padronized", tab), os.ModePerm)

			outFileName := fmt.Sprintf("%s_padronized/%s_%s_padronized.csv", tab, cadOuDoc, tab)
			if prefix != "" {
				outFileName = fmt.Sprintf("%s_padronized/%s_%s_padronized.csv", tab, prefix, aux)
			}

			outFile, err := os.Create(outFileName)
			if err != nil {
				return err
			}
			if err := merged.WriteCSV(outFile); err != nil {
				return err
			}
			outFile.Close()
			fmt.Printf("Arquivo %s gerado com sucesso!\n", outFileName)
		}
	}

	return nil
}

// transforma linhas em string para tentar reparar linhas com aspas soltas
func fixCsvLine(filename string, lineNum int, expectedCols int) []string {
	f, err := os.Open(filename)
	if err != nil {
		fmt.Printf("Não consegui reabrir %s: %v\n", filename, err)
		return nil
	}
	defer f.Close()

	reader := transform.NewReader(f, charmap.ISO8859_1.NewDecoder())
	scanner := bufio.NewScanner(reader)

	current := 0
	var line string
	for scanner.Scan() {
		current++
		if current == lineNum {
			line = scanner.Text()
			break
		}
	}

	if line == "" {
		return nil
	}

	line = strings.ReplaceAll(line, `;"`, ";")
	line = strings.ReplaceAll(line, `";`, ";")

	// Agora tentamos reparar
	r := csv.NewReader(strings.NewReader(line))
	r.Comma = ';'
	r.LazyQuotes = true

	row, err := r.Read()
	if err != nil {
		// fallback manual
		row = strings.Split(line, ";")
	}

	if len(row) > expectedCols {
		diff := len(row) - expectedCols
		for i, val := range row {
			if _, err := fmt.Sscanf(val, "%f", new(float64)); err != nil {
				mergedVal := strings.Join(row[i:i+diff+1], ";")
				newRow := append(row[:i], mergedVal)
				if i+diff+1 < len(row) {
					newRow = append(newRow, row[i+diff+1:]...)
				}
				row = newRow
				break
			}
		}
	}

	if len(row) != expectedCols {
		return nil
	}

	for i := range row {
		row[i] = strings.Trim(row[i], `"`)
	}

	fmt.Printf("Linha %d de %s reparada com sucesso!\n", lineNum, filename)
	return row
}

//	map[string]string{
//		"TP_FUNDO_CLASSE": "Não informado",
//		"ID_SUBCLASSE":    "",
//	},
//
//	map[string]string{
//		"CNPJ_FUNDO": "CNPJ_FUNDO_CLASSE",
//		"TP_FUNDO":   "TP_FUNDO_CLASSE",
//	},
//
// Adiciona uma coluna (TP_FUNDO_CLASSE) ou altera nomes de CNPJ_FUNDO e TP_FUNDO (caso já tenha) com valor fixo em todos os registros de um CSV e salva no mesmo arquivo
func addColumnToCsvFile(filePath string, mapColNameValue, mapPadronizado map[string]string) error {
	f, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer f.Close()

	reader := transform.NewReader(f, charmap.ISO8859_1.NewDecoder())
	r := csv.NewReader(reader)
	r.Comma = ';'
	r.LazyQuotes = true

	records, err := r.ReadAll()
	if err != nil {
		return err
	}
	df := dataframe.LoadRecords(records)

	// Adiciona todas as colunas do mapColNameValue
	for colName, colValue := range mapColNameValue {
		vals := make([]string, df.Nrow())
		for i := range vals {
			vals[i] = colValue
		}
		newCol := series.New(vals, series.String, colName)
		df = df.Mutate(newCol)
	}

	if mapPadronizado != nil {
		// Padroniza colunas de acordo com o mapPadronizado enviado
		fmt.Println("Padronizando colunas de", filePath)
		colMap := mapPadronizado
		for _, colNameOld := range df.Names() {
			if newName, ok := colMap[colNameOld]; ok && newName != colNameOld {
				df = df.Rename(newName, colNameOld)
			}
		}
	}

	out, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer out.Close()
	return df.WriteCSV(out)
}
