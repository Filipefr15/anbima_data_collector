package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/gocarina/gocsv"
)

type InfDiario struct {
	TPFundo     string `csv:"TP_FUNDO"`
	CNPJFundo   string `csv:"CNPJ_FUNDO"`
	DTComptc    string `csv:"DT_COMPTC"`
	VLTotal     string `csv:"VL_TOTAL"`
	VLQuota     string `csv:"VL_QUOTA"`
	VLPatrimLiq string `csv:"VL_PATRIM_LIQ"`
	CaptcDia    string `csv:"CAPTC_DIA"`
	ResgDia     string `csv:"RESG_DIA"`
	NrCotst     string `csv:"NR_COTST"`
}

func parseDate(s string) time.Time {
	t, err := time.Parse("2006-01-02", s)
	if err != nil {
		log.Fatal(err)
	}
	return t
}

func main() {
	folder := "./inf_diario"
	maxGoroutines := 20 // limite de goroutines simultâneas
	semaphore := make(chan struct{}, maxGoroutines)

	lastOfMonth := sync.Map{}
	var wg sync.WaitGroup

	err := filepath.Walk(folder, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() || filepath.Ext(path) != ".csv" {
			return nil
		}

		wg.Add(1)
		semaphore <- struct{}{} // bloqueia se limite atingido

		go func(p string) {
			defer wg.Done()
			defer func() { <-semaphore }() // libera vaga

			fmt.Println("Processando:", p)

			file, err := os.Open(p)
			if err != nil {
				log.Printf("Erro ao abrir %s: %v", p, err)
				return
			}
			defer file.Close()

			var records []InfDiario
			if err := gocsv.UnmarshalFile(file, &records); err != nil {
				log.Printf("Erro ao unmarshall %s: %v", p, err)
				return
			}

			for _, record := range records {
				date := parseDate(record.DTComptc)
				key := fmt.Sprintf("%s-%04d%02d", record.CNPJFundo, date.Year(), date.Month())

				actual, loaded := lastOfMonth.Load(key)
				if !loaded || date.After(parseDate(actual.(InfDiario).DTComptc)) {
					lastOfMonth.Store(key, record)
				}
			}
		}(path)

		return nil
	})

	if err != nil {
		log.Fatal(err)
	}

	wg.Wait()

	// Converte mapa para slice e ordena
	var consolidated []InfDiario
	lastOfMonth.Range(func(_, value interface{}) bool {
		consolidated = append(consolidated, value.(InfDiario))
		return true
	})

	sort.Slice(consolidated, func(i, j int) bool {
		if consolidated[i].CNPJFundo == consolidated[j].CNPJFundo {
			return consolidated[i].DTComptc < consolidated[j].DTComptc
		}
		return consolidated[i].CNPJFundo < consolidated[j].CNPJFundo
	})

	outFile, err := os.Create("consolidado.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer outFile.Close()

	if err := gocsv.MarshalFile(&consolidated, outFile); err != nil {
		log.Fatal("Erro ao exportar CSV:", err)
	}

	fmt.Println("CSV consolidado gerado com sucesso: consolidado.csv")
}
