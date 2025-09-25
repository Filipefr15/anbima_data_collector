package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"os"
	"strings"

	"github.com/go-gota/gota/dataframe"
	"golang.org/x/text/encoding/charmap"
	"golang.org/x/text/transform"
)

func csvPadronizationLamina2(tabs []string, anos, meses []int, prefix string, mapColNameValue, mapColumnToTransform map[string]string) error {
	for _, tab := range tabs {
		for _, ano := range anos {
			for _, mes := range meses {
				arquivo := fmt.Sprintf("lamina/lamina_fi%s%d%02d.csv", tab, ano, mes)
				if _, err := os.Stat(arquivo); err != nil {
					continue
				}

				f, err := os.Open(arquivo)
				if err != nil {
					fmt.Printf("Erro ao abrir arquivo %s: %v\n", arquivo, err)
					continue
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

					for i, val := range row {
						row[i] = strings.TrimPrefix(val, `"`)
						row[i] = strings.TrimSuffix(val, `"`)
					}

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
					os.MkdirAll("lamina_padronized", os.ModePerm)
					outFileName := fmt.Sprintf("lamina_padronized/lamina_fi%s%d%02d.csv", tab, ano, mes)
					outFile, err := os.Create(outFileName)
					if err != nil {
						fmt.Printf("Erro ao criar arquivo %s: %v\n", outFileName, err)
						continue
					}
					if err := df.WriteCSV(outFile); err != nil {
						fmt.Printf("Erro ao escrever CSV em %s: %v\n", outFileName, err)
					}
					outFile.Close()
					fmt.Printf("Arquivo %s gerado com sucesso!\n", outFileName)
				}
			}
		}
	}
	return nil
}
