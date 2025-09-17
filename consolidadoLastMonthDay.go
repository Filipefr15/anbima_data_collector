package main

import (
	"fmt"
	"os"
	"time"

	"github.com/go-gota/gota/dataframe"
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
