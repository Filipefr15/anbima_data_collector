package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
)

type Job struct {
	ano  int
	mes  int
	url  string
	file string
	dest string
}

func main() {
	fmt.Println("Selecione uma opção:")
	fmt.Println("1 - Iniciar downloads e descompactação")
	fmt.Println("2 - Organizar inf_diario e selecionar último dia de cada mês")
	fmt.Println("3 - Ligar pesquisa de info de fundos na porta 8080")
	fmt.Print("Digite 1, 2 ou 3: ")

	var escolha int
	_, err := fmt.Scan(&escolha)
	if err != nil {
		fmt.Println("Erro ao ler opção:", err)
		return
	}

	for {
		switch escolha {
		case 1:
			runDownloads()
		case 2:
			// err := organizeInfDiarioAndSelectLastDay([]int{2021, 2022, 2023, 2024})
			err := organizeInfDiarioAndSelectLastDay([]int{2021, 2022, 2023, 2024, 2025})
			if err != nil {
				fmt.Println("Erro ao organizar inf_diario:", err)
			}
		case 3:
			startServer()
		case 0:
			fmt.Println("Saindo...")
			return
		default:
			fmt.Println("Opção inválida.")
		}

		fmt.Println("\nSelecione uma opção:")
		fmt.Println("1 - Iniciar downloads e descompactação")
		fmt.Println("2 - Organizar inf_diario e selecionar último dia de cada mês")
		fmt.Println("3 - Ligar pesquisa de info de fundos na porta 8080")
		fmt.Println("0 - Sair")
		fmt.Print("Digite 1, 2, 3 ou 0: ")

		_, err := fmt.Scan(&escolha)
		if err != nil {
			fmt.Println("Erro ao ler opção:", err)
			return
		}
	}
}

func runDownloads() {
	var jobs []Job
	objeto_buscado := []string{"inf_diario", "lamina"}

	for _, objeto := range objeto_buscado {
		for ano := 2025; ano >= 2021; ano-- {
			for mes := 12; mes >= 1; mes-- {
				url := fmt.Sprintf("https://dados.cvm.gov.br/dados/FI/DOC/%s/DADOS/%s_fi_%d%02d.zip", objeto, objeto, ano, mes)
				output := fmt.Sprintf("%s_fi_%d%02d.zip", objeto, ano, mes)
				dest := objeto

				jobs = append(jobs, Job{
					ano:  ano,
					mes:  mes,
					url:  url,
					file: output,
					dest: dest,
				})
			}
		}
	}

	const maxWorkers = 12
	sem := make(chan struct{}, maxWorkers)
	var wg sync.WaitGroup

	for _, job := range jobs {
		wg.Add(1)
		go func(job Job) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			err := downloadFile(job.url, job.file)
			if err != nil {
				fmt.Println("Erro download:", err)
				if err := os.Remove(job.file); err != nil {
					fmt.Printf("Erro ao excluir o arquivo %s: %v\n", job.file, err)
				} else {
					fmt.Printf("Arquivo %s excluído com sucesso.\n", job.file)
				}
				return
			}

			err = unzip(job.file, job.dest)
			if err != nil {
				fmt.Println("Erro unzip:", err)
				return
			}
			fmt.Printf("Arquivo %s descompactado em: %s\n", job.file, job.dest)

			if err := os.Remove(job.file); err != nil {
				fmt.Printf("Erro ao excluir o arquivo %s: %v\n", job.file, err)
			} else {
				fmt.Printf("Arquivo %s excluído com sucesso.\n", job.file)
			}
		}(job)
	}

	wg.Wait()
	fmt.Println("Todos os downloads concluídos.")
}

type InfoDiario struct {
	TipoFundo     string
	CNPJ          string
	Data          string
	ValorTotal    string
	ValorQuota    string
	PatrimLiquido string
	CaptcDia      string
	ResgDia       string
	NumCotst      string
}

var infoDiarioCache map[string][]InfoDiario
var cacheLoaded bool
var cacheMutex sync.RWMutex

func normalizeCNPJ(cnpj string) string {
	return strings.NewReplacer(".", "", "-", "", "/", "").Replace(cnpj)
}

func loadInfoDiarioCache() error {
	dir := "inf_diario_ultimos_dias"
	files, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("erro ao ler diretório: %w", err)
	}

	cache := make(map[string][]InfoDiario)
	for _, file := range files {
		if file.IsDir() || !strings.HasPrefix(file.Name(), "consolidado_") || !strings.HasSuffix(file.Name(), "_ultimo_dia.csv") {
			continue
		}
		f, err := os.Open(dir + "/" + file.Name())
		if err != nil {
			continue
		}
		reader := csv.NewReader(f)
		reader.FieldsPerRecord = -1
		records, err := reader.ReadAll()
		f.Close()
		if err != nil {
			continue
		}
		for i, row := range records {
			if i == 0 {
				continue // pula header
			}
			if len(row) < 9 {
				continue
			}
			info := InfoDiario{
				TipoFundo:     row[0],
				CNPJ:          row[1],
				Data:          row[2],
				ValorTotal:    row[3],
				ValorQuota:    row[4],
				PatrimLiquido: row[5],
				CaptcDia:      row[6],
				ResgDia:       row[7],
				NumCotst:      row[8],
			}
			key := normalizeCNPJ(strings.TrimSpace(row[1]))
			cache[key] = append(cache[key], info)
		}
	}
	cacheMutex.Lock()
	infoDiarioCache = cache
	cacheLoaded = true
	cacheMutex.Unlock()
	return nil
}

func searchInfoHandler(w http.ResponseWriter, r *http.Request) {
	cnpj := r.URL.Query().Get("cnpj")
	if cnpj == "" {
		http.Error(w, "Parâmetro 'cnpj' é obrigatório", http.StatusBadRequest)
		return
	}

	year := r.URL.Query().Get("year")
	month := r.URL.Query().Get("month")

	normCNPJ := normalizeCNPJ(strings.TrimSpace(cnpj))

	cacheMutex.RLock()
	loaded := cacheLoaded
	allResults := infoDiarioCache[normCNPJ]
	cacheMutex.RUnlock()

	var results []InfoDiario
	for _, info := range allResults {
		if year != "" && (len(info.Data) < 4 || info.Data[:4] != year) {
			continue
		}
		if month != "" && (len(info.Data) < 7 || info.Data[5:7] != month) {
			continue
		}
		results = append(results, info)
	}

	w.Header().Set("Content-Type", "application/json")
	if !loaded || len(results) == 0 {
		w.Write([]byte("[]"))
		return
	}
	enc := json.NewEncoder(w)
	enc.Encode(results)
}

func startServer() {
	err := loadInfoDiarioCache()
	if err != nil {
		fmt.Println("Erro ao carregar cache:", err)
		return
	}
	http.HandleFunc("/searchInfo", searchInfoHandler)
	fmt.Println("Servidor iniciado em :8080")
	http.ListenAndServe(":8080", nil)
}
