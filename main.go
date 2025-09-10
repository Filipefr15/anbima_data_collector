package main

import (
	"fmt"
	"os"
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
	var jobs []Job
	objeto_buscado := []string{"inf_diario", "lamina"}

	for _, objeto := range objeto_buscado {
		for ano := 2024; ano >= 2021; ano-- {
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

	// número máximo de downloads simultâneos
	const maxWorkers = 12
	sem := make(chan struct{}, maxWorkers)
	var wg sync.WaitGroup

	for _, job := range jobs {
		wg.Add(1)
		go func(job Job) {
			defer wg.Done()

			// limita concorrência
			sem <- struct{}{}
			defer func() { <-sem }()

			err := downloadFile(job.url, job.file)
			if err != nil {
				fmt.Println("Erro download:", err)
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
