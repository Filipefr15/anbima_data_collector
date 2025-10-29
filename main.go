package main

import (
	"fmt"
)

func main() {
	fmt.Println("Selecione uma opção:")
	fmt.Println("1 - Iniciar downloads e descompactação")
	fmt.Println("2 - Organizar inf_diario e selecionar último dia de cada mês")
	fmt.Println("3 - Ligar pesquisa de info de fundos na porta 8080")
	fmt.Println("4 - Iniciar downloads e descompactação FIDC")
	fmt.Println("5 - Organizaar FIDC's")
	fmt.Println("6 - Organizar inf_diario com goroutines (versão melhorada)")
	fmt.Println("7 - Iniciar servidor com dados de AdmFii na porta 8080")
	fmt.Print("Digite 1, 2, 3, 4, 5, 6 ou 7: ")

	var escolha int
	_, err := fmt.Scan(&escolha)
	if err != nil {
		fmt.Println("Erro ao ler opção:", err)
		return
	}

	for {
		switch escolha {
		// ...existing code...
		case 1:
			runDownloads([]int{2021, 2022, 2023, 2024, 2025}, []string{"inf_diario"})
			fmt.Println("Informes diários baixados com sucesso.")
			runDownloads([]int{2019, 2020, 2021, 2022, 2023, 2024, 2025}, []string{"lamina"})
			fmt.Println("Lâminas baixadas com sucesso.")
		case 2:
			runDownloads([]int{2025}, []string{"inf_diario"})
			fmt.Println("Informes diários baixados com sucesso.")
			csvPadronizationInfDiario([]int{2025}, []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12})
			fmt.Println("Inf_diario organizado com sucesso!")
			pickLastDayOfMonthInfDiario([]int{2025}, []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12})
			fmt.Println("Último dia de cada mês selecionado com sucesso!")

			for anoMes := 202509; anoMes >= 202501; {
				database("inf_diario_ultimos_dias", fmt.Sprintf("csvs/inf_diario_ultimos_dias/inf_diario_fi_%d.csv", anoMes))
				// decrementa anoMes corretamente
				mes := anoMes % 100
				ano := anoMes / 100
				if mes == 1 {
					ano--
					mes = 12
				} else {
					mes--
				}
				anoMes = ano*100 + mes
			}
		case 3:

		case 4:
			runDownloadsFIDC([]int{2021, 2022, 2023, 2024, 2025}, []string{"fidc"})
			fmt.Println("FIDC's baixados com sucesso.")
		case 5:
			csvPadronizationFidc([]string{"_IV_", "_X_1_", "_X_2_", "_X_3_"}, []int{2021, 2022, 2023, 2024, 2025}, []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12})
			fmt.Println("FIDC's padronizados com sucesso.")
		case 6:
			err := csvPadronizationLamina(
				[]string{"_", "_carteira_", "_rentab_ano_", "_rentab_mes_"},
				[]int{2021, 2022, 2023, 2024, 2025},
				[]int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12},
			)
			if err != nil {
				fmt.Println("Erro ao organizar inf_diario (versão melhorada):", err)
			}
			fmt.Println("Inf_diario organizado com sucesso (versão melhorada)!")
		case 7:
			//startServerAdmFii()
		case 8:
			csvPadronizationFip([]string{"fip"}, []int{2019, 2020, 2021, 2022, 2023, 2024, 2025})
			fmt.Println("FIP's padronizados com sucesso.")
		case 9:
			runDownloadsFIP([]int{2019, 2020, 2021, 2022, 2023, 2024, 2025}, []string{"fip"})
			fmt.Println("FIP's baixados com sucesso.")
		case 10:

		case 11:
			downloadCsvDescompactado([]string{"adm_fii"}, "cad")
			fmt.Println("Cadastro de administradores de FII baixados com sucesso.")
			simpleCsvPadronization([]string{"adm_fii"}, []string{""}, "cad", "")
			fmt.Println("Cadastro de administradores de FII padronizados com sucesso.")
		case 12:
			downloadCsvDescompactado([]string{"fi"}, "cad")
			fmt.Println("Cadastro de informações de fundos baixados com sucesso.")
			simpleCsvPadronization([]string{"fi"}, []string{""}, "cad", "")
			fmt.Println("Cadastro de informações de fundos padronizados com sucesso.")
		case 13:
			downloadCsvCompactado([]string{"fi"}, "cad", "registro_fundo_classe")
			fmt.Println("Cadastro de informações de fundos (registro_fundo_classe) baixados com sucesso.")
			simpleCsvPadronization([]string{"fi"}, []string{"classe", "fundo", "subclasse"}, "cad", "registro")
		case 14:
			runDownloads([]int{2023, 2024, 2025}, []string{"cda"})
		case 15:
			csvPadronizationCda()
		case 16:
			// ex:
			// tableName := "cadastro_adm_fii"
			// csvFile := "adm_fii_padronized/cad_adm_fii.csv"
			// database("cadastro_adm_fii", "adm_fii_padronized/cad_adm_fii.csv")
			// database("cadastro_fi", "fi_padronized/cad_fi.csv")
			//database("cadastro_adm_fii", "adm_fii_padronized/cad_adm_fii.csv")
			//database("registro_classe", "fi_padronized/registro_classe.csv")
			//database("registro_fundo", "csvs/fi_padronized/registro_fundo.csv")
			// Adiciona todos os arquivos de csvs/cda_padronized no banco, nomeando pelo prefixo do arquivo
			prefixos := []string{
				// "cda_fi_BLC_1",
				// "cda_fi_BLC_2",
				// "cda_fi_BLC_3",
				// "cda_fi_BLC_4",
				// "cda_fi_BLC_5",
				// "cda_fi_BLC_6",
				"cda_fi_BLC_7",
				"cda_fi_BLC_8",
				"cda_fi_PL",
				"cda_fiim",
			}
			for _, prefixo := range prefixos {
				for ano := 2025; ano <= 2025; ano++ {
					for mes := 8; mes <= 8; mes++ {
						anoMes := fmt.Sprintf("%04d%02d", ano, mes)
						arquivos := []string{
							fmt.Sprintf("csvs/cda_padronized/%s_%s.csv", prefixo, anoMes),
						}
						for _, arquivo := range arquivos {
							// Extrai o nome do banco do prefixo do arquivo
							var tableName string
							if idx := len("csvs/cda_padronized/"); len(arquivo) > idx {
								rest := arquivo[idx:]
								if i := len(rest); i > 0 {
									// pega até o primeiro "_AAAA" (ano)
									for j := 0; j < i; j++ {
										if rest[j] == '_' && j+5 < i && rest[j+1] >= '0' && rest[j+1] <= '9' {
											tableName = rest[:j]
											break
										}
									}
								}
							}
							if tableName != "" {
								database(prefixo, arquivo)
							}
						}
					}
				}
			}
			//database("registro_subclasse", "fi_padronized/registro_subclasse.csv")
			//database("lamina_rentab_ano", "lamina_padronized/lamina_fi_rentab_ano_202508.csv")

			// for anoMes := 202508; anoMes >= 202101; {
			// 	database("inf_diario_ultimos_dias", fmt.Sprintf("inf_diario_ultimos_dias/inf_diario_fi_%d.csv", anoMes))
			// 	// decrementa anoMes corretamente
			// 	mes := anoMes % 100
			// 	ano := anoMes / 100
			// 	if mes == 1 {
			// 		ano--
			// 		mes = 12
			// 	} else {
			// 		mes--
			// 	}
			// 	anoMes = ano*100 + mes
			// }
		case 17:
			// database("cadastro_adm_fii", "csvs/adm_fii_padronized/cad_adm_fii.csv")
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
