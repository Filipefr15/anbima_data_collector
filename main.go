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
	fmt.Println("6 - Organizar inf_diario com goroutines (versão melhorada)")
	fmt.Print("Digite 1, 2, 3, 4, 5 ou 6: ")

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
			runDownloads([]int{2019, 2020, 2021, 2022, 2023, 2024, 2025}, []string{"lamina"})
			fmt.Println("Lâminas baixadas com sucesso.")

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
			csvPadronizationFidc([]string{"_IV_", "_X_1_", "_X_2_", "_X_3_"},
				[]int{2021, 2022, 2023, 2024, 2025},
				[]int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12},
				"fidc",
				map[string]string{
					"TP_FUNDO_CLASSE": "Não informado",
				},
				map[string]string{
					"CNPJ_FUNDO": "CNPJ_FUNDO_CLASSE",
					"TP_FUNDO":   "TP_FUNDO_CLASSE",
				})
			// err := organizeFIDCInfMensal([]int{2021, 2022, 2023, 2024, 2025})
			// if err != nil {
			// 	fmt.Println("Erro ao organizar inf_mensal FIDC:", err)
			// }
			// fmt.Println("FIDC's organizados com sucesso.")
			err = mashFIDCs([]int{2021, 2022, 2023, 2024, 2025})
			if err != nil {
				fmt.Println("Erro ao organizar FIDC's importantes:", err)
			}
			fmt.Println("FIDC's importantes organizados com sucesso.")
		case 6:
			err := csvPadronizationLamina(
				[]string{"_", "_carteira_", "_rentab_ano_", "_rentab_mes_"},
				[]int{2021, 2022, 2023, 2024, 2025},
				[]int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12},
				// "lamina",
				// map[string]string{
				// 	"TP_FUNDO_CLASSE": "Não informado",
				// 	"ID_SUBCLASSE":    "",
				// },
				// map[string]string{
				// 	"CNPJ_FUNDO": "CNPJ_FUNDO_CLASSE",
				// 	"TP_FUNDO":   "TP_FUNDO_CLASSE",
				// },
			)
			if err != nil {
				fmt.Println("Erro ao organizar inf_diario (versão melhorada):", err)
			}
			fmt.Println("Inf_diario organizado com sucesso (versão melhorada)!")
		case 7:
			organizeLaminas([]int{2021, 2022, 2023, 2024, 2025}, []string{"_", "_carteira_", "_rentab_ano_", "_rentab_mes_"})
			fmt.Println("Lâminas organizadas com sucesso.")
			mashLaminas([]int{2021, 2022, 2023, 2024, 2025}, []string{"_", "_carteira_", "_rentab_ano_", "_rentab_mes_"})
			fmt.Println("Lâminas consolidadas com sucesso.")
		case 8:
			runDownloadsFIP([]int{2019, 2020, 2021, 2022, 2023, 2024, 2025}, []string{"fip"})
			fmt.Println("FIP's baixados com sucesso.")
			organizeFIPs([]int{2019, 2020, 2021, 2022, 2023, 2024, 2025}, []string{"fip"})
			fmt.Println("FIP's organizados com sucesso.")
			mashFIPs([]int{2019, 2020, 2021, 2022, 2023, 2024, 2025}, []string{"fip"})
			fmt.Println("FIP's consolidados com sucesso.")
		case 9:
			startServer2()
		case 10:
			downloadCsvDescompactado([]string{"adm_fii"}, "cad")
			fmt.Println("Cadastro de administradores de FII baixados com sucesso.")
			simpleCsvPadronization([]string{"adm_fii"}, []string{""}, "cad", "")
			fmt.Println("Cadastro de administradores de FII padronizados com sucesso.")
		case 11:
			downloadCsvDescompactado([]string{"fi"}, "cad")
			fmt.Println("Cadastro de informações de fundos baixados com sucesso.")
			simpleCsvPadronization([]string{"fi"}, []string{""}, "cad", "")
			fmt.Println("Cadastro de informações de fundos padronizados com sucesso.")
		case 12:
			downloadCsvCompactado([]string{"fi"}, "cad", "registro_fundo_classe")
			fmt.Println("Cadastro de informações de fundos (registro_fundo_classe) baixados com sucesso.")
			//simpleCsvPadronization([]string{"fi"}, []string{"classe"}, "cad", "registro")
			simpleCsvPadronization([]string{"fi"}, []string{"classe", "fundo", "subclasse"}, "cad", "registro")
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
	// err := loadInfoDiarioCache()
	// if err != nil {
	// 	fmt.Println("Erro ao carregar cache:", err)
	// 	return
	// }

	// err = loadFIDCCache()
	// if err != nil {
	// 	fmt.Println("Erro ao carregar cache FIDC:", err)
	// 	return
	// }

	err := loadLaminaCache([]int{2020, 2021, 2022, 2023, 2024, 2025})
	if err != nil {
		fmt.Println("Erro ao carregar cache Lâminas:", err)
		return
	}

	http.HandleFunc("/searchInfo", searchInfoHandler)
	fmt.Println("search info carregado")
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
