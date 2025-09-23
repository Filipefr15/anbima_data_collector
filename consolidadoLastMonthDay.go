package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"
	"golang.org/x/text/encoding/charmap"
	"golang.org/x/text/transform"
)

// Organiza os dados de inf_diario e seleciona apenas o último dia de cada mês para cada CNPJ
func organizeInfDiarioAndSelectLastDay(anos []int) error {
	// anos := []int{2021, 2022, 2023, 2024}
	for _, ano := range anos {
		var merged dataframe.DataFrame
		first := true

		for mes := 1; mes <= 12; mes++ {
			arquivo := fmt.Sprintf("inf_diario/inf_diario_fi_%d%02d.csv", ano, mes)
			if _, err := os.Stat(arquivo); err != nil {
				continue
			}
			f, err := os.Open(arquivo)
			if err != nil {
				return err
			}
			df := dataframe.ReadCSV(f, dataframe.WithDelimiter(';'))
			f.Close()

			// Normaliza colunas
			colMap := map[string]string{
				"TP_FUNDO":   "TP_FUNDO_CLASSE",
				"CNPJ_FUNDO": "CNPJ_FUNDO_CLASSE",
			}
			for _, colName := range df.Names() {
				if newName, ok := colMap[colName]; ok && newName != colName {
					df = df.Rename(newName, colName)
				}
			}
			for _, colName := range df.Names() {
				if colName == "ID_SUBCLASSE" {
					df = df.Drop("ID_SUBCLASSE")
					break
				}
			}

			// Converte DT_COMPTC para time.Time
			dates := df.Col("DT_COMPTC").Records()
			parsed := make([]time.Time, len(dates))
			for i, d := range dates {
				t, _ := time.Parse("2006-01-02", d)
				parsed[i] = t
			}

			// Última data por CNPJ
			cnpjs := df.Col("CNPJ_FUNDO_CLASSE").Records()
			lastRows := []int{}
			lastSeen := map[string]time.Time{}
			rowIdx := map[string]int{}

			for i, cnpj := range cnpjs {
				curDate := parsed[i]
				if curDate.After(lastSeen[cnpj]) {
					lastSeen[cnpj] = curDate
					rowIdx[cnpj] = i
				}
			}
			for _, idx := range rowIdx {
				lastRows = append(lastRows, idx)
			}
			df = df.Subset(lastRows)

			if first {
				merged = df
				first = false
			} else {
				merged = merged.RBind(df)
			}
		}

		if merged.Nrow() == 0 {
			fmt.Printf("Nenhum dado encontrado para o ano %d\n", ano)
			continue
		}

		outFileName := fmt.Sprintf("inf_diario_ultimos_dias/consolidado_%d_ultimo_dia.csv", ano)
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
	return nil
}

// Organiza os dados dos FIDCS e seleciona apenas o último dia de cada mês para cada CNPJ
func organizeFIDCInfMensal(anos []int) error {
	fidc_tabs := []string{"_IV_", "_X_1_", "_X_2_", "_X_3_"}

	for _, fidc := range fidc_tabs {
		for _, ano := range anos {
			var merged dataframe.DataFrame
			first := true

			dfCh := make(chan dataframe.DataFrame)
			var wg sync.WaitGroup

			for mes := 1; mes <= 12; mes++ {
				arquivo := fmt.Sprintf("fidc/inf_mensal_fidc_tab%s%d%02d.csv", fidc, ano, mes)
				if _, err := os.Stat(arquivo); err != nil {
					continue
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
					r := csv.NewReader(reader)
					r.Comma = ';'
					r.LazyQuotes = true

					var records [][]string
					var header []string

					for {
						row, err := r.Read()
						if err != nil {
							if err == io.EOF {
								break
							}
							fmt.Printf("Erro ao ler linha em %s: %v\n", arquivo, err)
							break
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
							fmt.Printf("Ignorando linha irrecuperável em %s: %v\n", arquivo, row)
							continue
						}

						records = append(records, row)
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
				fmt.Printf("Nenhum dado encontrado para o ano %d\n", ano)
				continue
			}
			// Normaliza colunas
			colMap := map[string]string{
				"CNPJ_FUNDO": "CNPJ_FUNDO_CLASSE",
			}
			for _, colName := range merged.Names() {
				if newName, ok := colMap[colName]; ok && newName != colName {
					merged = merged.Rename(newName, colName)
				}
			}
			colExists := false
			for _, colName := range merged.Names() {
				if colName == "TP_FUNDO_CLASSE" {
					colExists = true
					break
				}
			}

			if !colExists {
				vals := make([]string, merged.Nrow())
				for i := range vals {
					vals[i] = "Não informado"
				}
				newCol := series.New(vals, series.String, "TP_FUNDO_CLASSE")
				merged = merged.Mutate(newCol)
			}

			switch fidc {
			case "_IV_":
				//TODO
			case "_X_1_":
				//TODO
			case "_X_2_":
				//TODO
			case "_X_3_":
				//TODO
			default:
				fmt.Println("FIDC desconhecido:", fidc)
			}

			outFileName := fmt.Sprintf("fidc_mensal_anualizado/fidc_mensal_%d%s.csv", ano, fidc)
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

func mashFIDCs(anos []int) error {
	// anos := []int{2021, 2022, 2023, 2024}
	fidc_tabs := []string{"_IV_", "_X_1_", "_X_2_", "_X_3_"}
	for _, fidc := range fidc_tabs {
		var merged dataframe.DataFrame
		first := true
		for _, ano := range anos {
			arquivo := fmt.Sprintf("fidc_mensal_anualizado/fidc_mensal_%d%s.csv", ano, fidc)
			if _, err := os.Stat(arquivo); err != nil {
				continue
			}
			f, err := os.Open(arquivo)
			if err != nil {
				return err
			}
			df := dataframe.ReadCSV(f)
			f.Close()

			if first {
				merged = df
				first = false
			} else {
				merged = merged.RBind(df)
			}

		}
		if merged.Nrow() == 0 {
			fmt.Printf("Nenhum dado encontrado para o FIDC %s\n", fidc)
			continue
		}

		outFileName := fmt.Sprintf("fidcs_anualizados_juntados/fidc_consolidado%s.csv", fidc)
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
	return nil
}

func mashFIDCsIntoOne(fidcs []string) error {

	// mapa chave -> todas as colunas agregadas
	joined := make(map[string]map[string]string)

	for _, fidc := range fidcs {
		file := fmt.Sprintf("fidcs_anualizados_juntados/fidc_consolidado%s.csv", fidc)
		f, err := os.Open(file)
		if err != nil {
			return fmt.Errorf("erro abrindo %s: %w", file, err)
		}
		defer f.Close()

		df := dataframe.ReadCSV(f, dataframe.WithDelimiter(',')) // se precisar: ';'
		cols := df.Names()

		for i := 0; i < df.Nrow(); i++ {
			row := df.Subset(i)

			cnpj := row.Col("CNPJ_FUNDO_CLASSE").Elem(0).String()
			denom := row.Col("DENOM_SOCIAL").Elem(0).String()
			dt := row.Col("DT_COMPTC").Elem(0).String()

			key := fmt.Sprintf("%s|%s|%s", cnpj, denom, dt)

			if _, ok := joined[key]; !ok {
				joined[key] = map[string]string{
					"CNPJ_FUNDO_CLASSE": cnpj,
					"DENOM_SOCIAL":      denom,
					"DT_COMPTC":         dt,
				}
			}

			for _, c := range cols {
				if c != "CNPJ_FUNDO_CLASSE" && c != "DENOM_SOCIAL" && c != "DT_COMPTC" {
					joined[key][c] = row.Col(c).Elem(0).String()
				}
			}
		}
	}

	// montar CSV de saída
	out, err := os.Create("fidcs_anualizados_juntados/fidc_consolidado_FINAL.csv")
	if err != nil {
		return err
	}
	defer out.Close()
	writer := csv.NewWriter(out)

	// cabeçalhos dinâmicos
	headers := []string{"CNPJ_FUNDO_CLASSE", "DENOM_SOCIAL", "DT_COMPTC"}
	colset := make(map[string]bool)
	for _, row := range joined {
		for c := range row {
			if c != "CNPJ_FUNDO_CLASSE" && c != "DENOM_SOCIAL" && c != "DT_COMPTC" {
				colset[c] = true
			}
		}
	}
	for c := range colset {
		headers = append(headers, c)
	}
	if err := writer.Write(headers); err != nil {
		return err
	}

	// linhas
	for _, row := range joined {
		line := make([]string, len(headers))
		for i, h := range headers {
			line[i] = row[h]
		}
		if err := writer.Write(line); err != nil {
			return err
		}
	}
	writer.Flush()

	fmt.Println("fidc_consolidado_FINAL.csv gerado com sucesso!")
	return nil
}

func organizeLaminas(anos []int, tabs []string) error {
	//laminasTabs := []string{"_", "_carteira_", "_rentab_ano_", "_rentab_mes_"}

	for _, lamina := range tabs {
		for _, ano := range anos {
			var merged dataframe.DataFrame
			first := true

			dfCh := make(chan dataframe.DataFrame)
			var wg sync.WaitGroup

			for mes := 1; mes <= 12; mes++ {
				arquivo := fmt.Sprintf("lamina/lamina_fi%s%d%02d.csv", lamina, ano, mes)
				if _, err := os.Stat(arquivo); err != nil {
					continue
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
					r := csv.NewReader(reader)
					r.Comma = ';'
					r.LazyQuotes = true

					var records [][]string
					var header []string

					for {
						row, err := r.Read()
						if err != nil {
							if err == io.EOF {
								break
							}
							fmt.Printf("Erro ao ler linha em %s: %v\n", arquivo, err)
							break
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
							fmt.Printf("Ignorando linha irrecuperável em %s: %v\n", arquivo, row)
							continue
						}

						records = append(records, row)
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

						colExists := false
						for _, colName := range df.Names() {
							if colName == "TP_FUNDO_CLASSE" {
								colExists = true
								break
							}
						}
						if !colExists {
							vals := make([]string, df.Nrow())
							for i := range vals {
								vals[i] = "Não informado"
							}
							newCol := series.New(vals, series.String, "TP_FUNDO_CLASSE")
							df = df.Mutate(newCol)
						}

						colExists = false
						for _, colName := range df.Names() {
							if colName == "ID_SUBCLASSE" {
								colExists = true
								break
							}
						}
						if !colExists {
							vals := make([]string, df.Nrow())
							for i := range vals {
								vals[i] = ""
							}
							newCol := series.New(vals, series.String, "ID_SUBCLASSE")
							df = df.Mutate(newCol)
						}

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
				fmt.Printf("Nenhum dado encontrado para o ano %d\n", ano)
				continue
			}

			outFileName := fmt.Sprintf("lamina_consolidado/lamina%s%d.csv", lamina, ano)
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

func mashLaminas(anos []int, tabs []string) error {
	// anos := []int{2021, 2022, 2023, 2024}
	//laminasTabs := []string{"_", "_carteira_", "_rentab_ano_", "_rentab_mes_"}
	for _, tab := range tabs {
		var merged dataframe.DataFrame
		first := true
		for _, ano := range anos {
			arquivo := fmt.Sprintf("lamina_consolidado/lamina%s%d.csv", tab, ano)
			if _, err := os.Stat(arquivo); err != nil {
				continue
			}
			f, err := os.Open(arquivo)
			if err != nil {
				return err
			}
			df := dataframe.ReadCSV(f)
			f.Close()

			if first {
				merged = df
				first = false
			} else {
				merged = merged.RBind(df)
			}

		}
		if merged.Nrow() == 0 {
			fmt.Printf("Nenhum dado encontrado para a Lâmina %s\n", tab)
			continue
		}
		os.MkdirAll("lamina_final", os.ModePerm)
		outFileName := fmt.Sprintf("lamina_final/lamina_final%s.csv", tab)
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
	return nil
}

func organizeFIPs(anos []int, tabs []string) error {
	//laminasTabs := []string{"_", "_carteira_", "_rentab_ano_", "_rentab_mes_"}

	for _, fip := range tabs {
		for _, ano := range anos {
			var merged dataframe.DataFrame
			first := true

			dfCh := make(chan dataframe.DataFrame)
			var wg sync.WaitGroup

			arquivo := fmt.Sprintf("%s/inf_tri_quadri_%s_%d.csv", fip, fip, ano)
			if _, err := os.Stat(arquivo); err != nil {
				continue
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
				r := csv.NewReader(reader)
				r.Comma = ';'
				r.LazyQuotes = true

				var records [][]string
				var header []string

				for {
					row, err := r.Read()
					if err != nil {
						if err == io.EOF {
							break
						}
						fmt.Printf("Erro ao ler linha em %s: %v\n", arquivo, err)
						break
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
						fmt.Printf("Ignorando linha irrecuperável em %s: %v\n", arquivo, row)
						continue
					}

					records = append(records, row)
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

					colExists := false
					for _, colName := range df.Names() {
						if colName == "TP_FUNDO_CLASSE" {
							colExists = true
							break
						}
					}
					if !colExists {
						vals := make([]string, df.Nrow())
						for i := range vals {
							vals[i] = "FIP"
						}
						newCol := series.New(vals, series.String, "TP_FUNDO_CLASSE")
						df = df.Mutate(newCol)
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
				fmt.Printf("Nenhum dado encontrado para o ano %d\n", ano)
				continue
			}

			outFileName := fmt.Sprintf("%s/inf_tri_quadri_%s_%d_organizados.csv", fip, fip, ano)
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

func mashFIPs(anos []int, tabs []string) error {
	// anos := []int{2021, 2022, 2023, 2024}
	//laminasTabs := []string{"_", "_carteira_", "_rentab_ano_", "_rentab_mes_"}
	for _, tab := range tabs {
		var merged dataframe.DataFrame
		first := true
		for _, ano := range anos {
			arquivo := fmt.Sprintf("%s/inf_tri_quadri_%s_%d_organizados.csv", tab, tab, ano)
			if _, err := os.Stat(arquivo); err != nil {
				continue
			}
			f, err := os.Open(arquivo)
			if err != nil {
				return err
			}
			df := dataframe.ReadCSV(f)
			f.Close()

			if first {
				merged = df
				first = false
			} else {
				merged = merged.RBind(df)
			}

		}
		if merged.Nrow() == 0 {
			fmt.Printf("Nenhum dado encontrado para o Fip %s\n", tab)
			continue
		}
		os.MkdirAll(fmt.Sprintf("%s_final", tab), os.ModePerm)
		outFileName := fmt.Sprintf("%s_final/inf_tri_quadri_%s_geral.csv", tab, tab)
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
	return nil
}

func fidcTeste(anos []int, tabs []string) error {
	//fidc_tabs := []string{"_IV_", "_X_1_", "_X_2_", "_X_3_"}

	for _, tab := range tabs {
		for _, ano := range anos {
			var merged dataframe.DataFrame
			first := true

			dfCh := make(chan dataframe.DataFrame)
			var wg sync.WaitGroup

			for mes := 1; mes <= 12; mes++ {
				arquivo := fmt.Sprintf("fidc/inf_mensal_fidc_tab%s%d%02d.csv", tab, ano, mes)
				if _, err := os.Stat(arquivo); err != nil {
					continue
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
					r := csv.NewReader(reader)
					r.Comma = ';'
					r.LazyQuotes = true

					var records [][]string
					var header []string

					for {
						row, err := r.Read()
						if err != nil {
							if err == io.EOF {
								break
							}
							fmt.Printf("Erro ao ler linha em %s: %v\n", arquivo, err)
							break
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
							fmt.Printf("Ignorando linha irrecuperável em %s: %v\n", arquivo, row)
							continue
						}

						records = append(records, row)
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
				fmt.Printf("Nenhum dado encontrado para o ano %d\n", ano)
				continue
			}
			// Normaliza colunas
			colMap := map[string]string{
				"CNPJ_FUNDO": "CNPJ_FUNDO_CLASSE",
			}
			for _, colName := range merged.Names() {
				if newName, ok := colMap[colName]; ok && newName != colName {
					merged = merged.Rename(newName, colName)
				}
			}
			colExists := false
			for _, colName := range merged.Names() {
				if colName == "TP_FUNDO_CLASSE" {
					colExists = true
					break
				}
			}

			if !colExists {
				vals := make([]string, merged.Nrow())
				for i := range vals {
					vals[i] = "Não informado"
				}
				newCol := series.New(vals, series.String, "TP_FUNDO_CLASSE")
				merged = merged.Mutate(newCol)
			}

			outFileName := fmt.Sprintf("fidc_mensal_anualizado/fidc_mensal_%d%s.csv", ano, tab)
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

// prefix para fidc => fidc
func csvPadronization(tabs []string, auxs, meses []int, cadOuDoc, prefix string) error {
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
							standardizeHeaders(header)
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
				fmt.Printf("Nenhum dado encontrado para %s no ano %d\n", tab, aux)
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

// Padroniza os headers e garante a existência da coluna TP_FUNDO_CLASSE
// Padroniza colunas e garante a existência da coluna TP_FUNDO_CLASSE
// Padroniza os headers e garante a existência da coluna TP_FUNDO_CLASSE
func standardizeHeaders(header []string) []string {
	colMap := map[string]string{
		"CNPJ_FUNDO": "CNPJ_FUNDO_CLASSE",
		"TP_FUNDO":   "TP_FUNDO_CLASSE",
	}
	for i, col := range header {
		if newName, ok := colMap[col]; ok && newName != col {
			header[i] = newName
		}
	}
	// Garante existência da coluna TP_FUNDO_CLASSE
	hasTpFundoClasse := false
	for _, col := range header {
		if col == "TP_FUNDO_CLASSE" {
			hasTpFundoClasse = true
			break
		}
	}
	if !hasTpFundoClasse {
		header = append(header, "TP_FUNDO_CLASSE")
		fmt.Println("Adicionada coluna TP_FUNDO_CLASSE", header)
	}
	return header
}
