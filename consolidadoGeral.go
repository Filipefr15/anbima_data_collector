package main

import (
	"fmt"
	"log"
	"os"

	"github.com/go-gota/gota/dataframe"
)

// Função que transforma os arquivos em
func consolidadoGeral() {
	// Map de anos para lista de arquivos
	anos := []int{2021, 2022}
	for _, ano := range anos {
		var merged dataframe.DataFrame
		first := true

		// Percorre todos os meses
		for mes := 1; mes <= 12; mes++ {
			// Monta o nome do arquivo
			arquivo := fmt.Sprintf("inf_diario/inf_diario_fi_%d%02d.csv", ano, mes)
			// Verifica se o arquivo existe
			if _, err := os.Stat(arquivo); err != nil {
				continue // pula meses que não existem
			}
			f, err := os.Open(arquivo)
			if err != nil {
				log.Fatal(err)
			}

			// Lê o arquivo CSV
			df := dataframe.ReadCSV(f, dataframe.WithDelimiter(';'))
			f.Close()

			// Renomeia colunas para o padrão do segundo formato (exceto ID_SUBCLASSE)
			colMap := map[string]string{
				"TP_FUNDO":          "TP_FUNDO_CLASSE",
				"CNPJ_FUNDO":        "CNPJ_FUNDO_CLASSE",
				"DT_COMPTC":         "DT_COMPTC",
				"VL_TOTAL":          "VL_TOTAL",
				"VL_QUOTA":          "VL_QUOTA",
				"VL_PATRIM_LIQ":     "VL_PATRIM_LIQ",
				"CAPTC_DIA":         "CAPTC_DIA",
				"RESG_DIA":          "RESG_DIA",
				"NR_COTST":          "NR_COTST",
				"TP_FUNDO_CLASSE":   "TP_FUNDO_CLASSE",
				"CNPJ_FUNDO_CLASSE": "CNPJ_FUNDO_CLASSE",
			}

			// Renomeia as colunas conforme o mapeamento
			for _, colName := range df.Names() {
				if newName, ok := colMap[colName]; ok && newName != colName {
					df = df.Rename(newName, colName)
				}
			}

			// Remove a coluna ID_SUBCLASSE, se existir
			for _, colName := range df.Names() {
				if colName == "ID_SUBCLASSE" {
					df = df.Drop("ID_SUBCLASSE")
					break
				}
			}

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

		outFileName := fmt.Sprintf("/inf_diario_consolidado/consolidado_%d.csv", ano)
		outFile, err := os.Create(outFileName)
		if err != nil {
			log.Fatal(err)
		}

		if err := merged.WriteCSV(outFile); err != nil {
			log.Fatal(err)
		}
		outFile.Close()
		fmt.Printf("Arquivo %s gerado com sucesso!\n", outFileName)
	}
}
