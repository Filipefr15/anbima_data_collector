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
	aux  string
}

func runDownloads(anos []int, objetoBuscado []string) {
	var jobs []Job

	for _, objeto := range objetoBuscado {
		for _, ano := range anos {
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

func runDownloadsFIDC(anos []int, objetoBuscado []string) {
	var jobs []Job

	for _, objeto := range objetoBuscado {
		for _, ano := range anos {
			for mes := 12; mes >= 1; mes-- {
				url := fmt.Sprintf("https://dados.cvm.gov.br/dados/%s/DOC/INF_MENSAL/DADOS/inf_mensal_%s_%d%02d.zip", objeto, objeto, ano, mes)
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

func runDownloadsFIP(anos []int, objetoBuscado []string) {
	var jobs []Job

	for _, objeto := range objetoBuscado {
		for _, ano := range anos {
			periodicidade_informe := "inf_trimestral"
			if ano > 2023 {
				periodicidade_informe = "inf_quadrimestral"
			}
			url := fmt.Sprintf("https://dados.cvm.gov.br/dados/%s/DOC/%s/DADOS/%s_%s_%d.csv", objeto, periodicidade_informe, periodicidade_informe, objeto, ano)
			output := fmt.Sprintf("%s_%s_%d.csv", periodicidade_informe, objeto, ano)
			dest := objeto

			jobs = append(jobs, Job{
				ano:  ano,
				mes:  01,
				url:  url,
				file: output,
				dest: dest,
				aux:  fmt.Sprintf("%s_%s_%d.csv", "inf_tri_quadri", objeto, ano),
			})
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

			// err = unzip(job.file, job.dest)
			// if err != nil {
			// 	fmt.Println("Erro unzip:", err)
			// 	return
			// }

			if _, err := os.Stat(job.dest); os.IsNotExist(err) {
				err = os.MkdirAll(job.dest, 0755)
				if err != nil {
					fmt.Println("Erro ao criar diretório:", err)
					return
				}
			}

			err = os.Rename(job.file, fmt.Sprintf("%s/%s", job.dest, job.aux))
			if err != nil {
				fmt.Println("Erro ao mover arquivo:", err)
				return
			}

			// err = os.Rename(fmt.Sprintf("%s/%s", job.dest, job.file), job.aux)
			// if err != nil {
			// 	fmt.Println("Erro ao mover arquivo:", err)
			// 	return
			// }
			// os.MkdirAll(job.dest, 0755)

			// os.Mkdir(job.dest, 0755)
			fmt.Printf("Arquivo %s descompactado em: %s\n", job.file, job.dest)

			// if err := os.Remove(job.file); err != nil {
			// 	fmt.Printf("Erro ao excluir o arquivo %s: %v\n", job.file, err)
			// } else {
			// 	fmt.Printf("Arquivo %s excluído com sucesso.\n", job.file)
			// }
		}(job)
	}

	wg.Wait()
	fmt.Println("Todos os downloads concluídos.")
}
