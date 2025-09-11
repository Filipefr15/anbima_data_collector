package main

import (
	"fmt"
	"log"
	"os"

	"github.com/go-gota/gota/dataframe"
)

func main() {
	// Map de anos para lista de arquivos
	anos := []int{2021, 2022, 2023, 2024}
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
			defer f.Close()

			df := dataframe.ReadCSV(f, dataframe.WithDelimiter(';'))
			if first {
				merged = df
				first = false
			} else {
				merged = merged.RBind(df)
			}
		}

		outFileName := fmt.Sprintf("consolidado_%d.csv", ano)
		outFile, err := os.Create(outFileName)
		if err != nil {
			log.Fatal(err)
		}
		defer outFile.Close()

		if err := merged.WriteCSV(outFile); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Arquivo %s gerado com sucesso!\n", outFileName)

	}
}
