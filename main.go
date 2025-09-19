package main

import (
	"fmt"
	"net/http"
)

func main() {
	fmt.Println("Selecione uma opção:")
	fmt.Println("1 - Iniciar downloads e descompactação")
	fmt.Println("2 - Organizar inf_diario e selecionar último dia de cada mês")
	fmt.Println("3 - Ligar pesquisa de info de fundos na porta 8080")
	fmt.Println("4 - Iniciar downloads e descompactação FIDC")
	fmt.Println("5 - Organizaar FIDC's")
	fmt.Print("Digite 1, 2, 3, 4 ou 5: ")

	var escolha int
	_, err := fmt.Scan(&escolha)
	if err != nil {
		fmt.Println("Erro ao ler opção:", err)
		return
	}

	for {
		switch escolha {
		case 1:
			// runDownloads([]int{2021, 2022, 2023, 2024, 2025}, []string{"inf_diario"})
			// fmt.Println("Informes diários baixados com sucesso.")
			// runDownloads([]int{2019, 2020, 2021, 2022, 2023, 2024, 2025}, []string{"lamina"})
			// fmt.Println("Lâminas baixadas com sucesso.")
			runDownloadsFIP([]int{2019, 2020, 2021, 2022, 2023, 2024, 2025}, []string{"fip"})
			fmt.Println("FIP's baixados com sucesso.")
		case 2:
			// err := organizeInfDiarioAndSelectLastDay([]int{2021, 2022, 2023, 2024})
			err := organizeInfDiarioAndSelectLastDay([]int{2021, 2022, 2023, 2024, 2025})
			if err != nil {
				fmt.Println("Erro ao organizar inf_diario:", err)
			}
		case 3:
			startServer()
		case 4:
			runDownloadsFIDC([]int{2021, 2022, 2023, 2024, 2025}, []string{"fidc"})
			fmt.Println("FIDC's baixados com sucesso.")
		case 5:
			err := organizeFIDCInfMensal([]int{2021, 2022, 2023, 2024, 2025})
			if err != nil {
				fmt.Println("Erro ao organizar inf_mensal FIDC:", err)
			}
			fmt.Println("FIDC's organizados com sucesso.")
			err = mashFIDCs([]int{2021, 2022, 2023, 2024, 2025})
			if err != nil {
				fmt.Println("Erro ao organizar FIDC's importantes:", err)
			}
			fmt.Println("FIDC's importantes organizados com sucesso.")
		case 6:
			mashFIDCsIntoOne([]string{"_IV_", "_X_1_", "_X_2_", "_X_3_"})
			fmt.Println("FIDC's consolidados com sucesso.")
		case 7:
			organizeLaminas([]int{2021, 2022, 2023, 2024, 2025}, []string{"_", "_carteira_", "_rentab_ano_", "_rentab_mes_"})
			fmt.Println("Lâminas organizadas com sucesso.")
			mashLaminas([]int{2021, 2022, 2023, 2024, 2025}, []string{"_", "_carteira_", "_rentab_ano_", "_rentab_mes_"})
			fmt.Println("Lâminas consolidadas com sucesso.")
		case 8:
			organizeFIPs([]int{2019, 2020, 2021, 2022, 2023, 2024, 2025}, []string{"fip"})
			fmt.Println("FIP's organizados com sucesso.")
			mashFIPs([]int{2019, 2020, 2021, 2022, 2023, 2024, 2025}, []string{"fip"})
			fmt.Println("FIP's consolidados com sucesso.")
		case 9:
			startServer2()
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
		fmt.Println("4 - Iniciar downloads e descompactação FIDC")
		fmt.Println("5 - Organizaar FIDC's")
		fmt.Println("0 - Sair")
		fmt.Print("Digite 1, 2, 3, 4, 5 ou 0: ")

		_, err := fmt.Scan(&escolha)
		if err != nil {
			fmt.Println("Erro ao ler opção:", err)
			return
		}
	}
}

func startServer() {
	err := loadInfoDiarioCache()
	if err != nil {
		fmt.Println("Erro ao carregar cache:", err)
		return
	}

	err = loadFIDCCache()
	if err != nil {
		fmt.Println("Erro ao carregar cache FIDC:", err)
		return
	}

	err = loadLaminaCache()
	if err != nil {
		fmt.Println("Erro ao carregar cache Lâminas:", err)
		return
	}

	http.HandleFunc("/searchInfo", searchInfoHandler)
	http.HandleFunc("/searchFIDC", searchFIDCHandler)
	http.HandleFunc("/searchLamina", searchLaminaHandler)
	fmt.Println("Servidor iniciado em :8080")
	http.ListenAndServe(":8080", nil)
}

func startServer2() {
	err := loadFipCache()
	if err != nil {
		fmt.Println("Erro ao carregar cache FIDC:", err)
		return
	}

	http.HandleFunc("/searchFip", searchFipHandler)
	fmt.Println("Servidor iniciado em :8080")
	http.ListenAndServe(":8080", nil)
}
