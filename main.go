package main

import (
	"dAndD/models"
	"encoding/json"
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
			startServer()
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
			startServerRegistroFundo()
		case 8:
			csvPadronizationFip([]string{"fip"}, []int{2019, 2020, 2021, 2022, 2023, 2024, 2025})
			fmt.Println("FIP's padronizados com sucesso.")
		case 9:
			runDownloadsFIP([]int{2019, 2020, 2021, 2022, 2023, 2024, 2025}, []string{"fip"})
			fmt.Println("FIP's baixados com sucesso.")
		case 10:
			startServer2()
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
			database("registro_fundo", "csvs/fi_padronized/registro_fundo.csv")
			// Adiciona todos os arquivos de csvs/cda_padronized no banco, nomeando pelo prefixo do arquivo
			for ano := 2021; ano <= 2025; ano++ {
				for mes := 1; mes <= 12; mes++ {
					anoMes := fmt.Sprintf("%04d%02d", ano, mes)
					arquivos := []string{
						fmt.Sprintf("csvs/cda_padronized/cda_%s.csv", anoMes),
						fmt.Sprintf("csvs/cda_padronized/cda_fi_%s.csv", anoMes),
						fmt.Sprintf("csvs/cda_padronized/cda_fidc_%s.csv", anoMes),
						fmt.Sprintf("csvs/cda_padronized/cda_fip_%s.csv", anoMes),
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
							database(tableName, arquivo)
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

	err := loadFIDCCache()
	if err != nil {
		fmt.Println("Erro ao carregar cache FIDC:", err)
		return
	}

	// err := loadLaminaCache([]int{2020, 2021, 2022, 2023, 2024, 2025})
	// if err != nil {
	// 	fmt.Println("Erro ao carregar cache Lâminas:", err)
	// 	return
	// }

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

	err = loadCdaCache()
	if err != nil {
		fmt.Println("Erro ao carregar cache CDA:", err)
		return
	}

	http.HandleFunc("/searchFip", searchFipHandler)
	http.HandleFunc("/searchCda", searchCdaHandler)
	fmt.Println("Servidor iniciado em :8080")
	http.ListenAndServe(":8080", nil)
}

func startServerAdmFii() {
	http.HandleFunc("/admfii", admFiiHandler)
	fmt.Println("Servidor AdmFii iniciado em :8080")
	fmt.Println("Acesse: http://localhost:8080/admfii")
	http.ListenAndServe(":8080", nil)
}

func startServerRegistroFundo() {
	http.HandleFunc("/registrofundo", registroFundoHandler)
	fmt.Println("Servidor RegistroFundo iniciado em :8080")
	fmt.Println("Acesse: http://localhost:8080/registrofundo")
	http.ListenAndServe(":8080", nil)
}

func registroFundoHandler(w http.ResponseWriter, r *http.Request) {
	// Conecta ao banco
	db, err := conectaDB()
	if err != nil {
		http.Error(w, "Erro ao conectar ao banco de dados: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer db.Close()

	// Executa a consulta
	rows, err := executarConsulta(db, "sql/registro_fundo.sql")
	if err != nil {
		http.Error(w, "Erro ao executar consulta: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	// Processa os resultados
	var fundos []models.RegistroFundo
	for rows.Next() {
		var fundo models.RegistroFundo
		err = rows.Scan(
			&fundo.IdRegistroFundo,
			&fundo.CNPJFundo,
			&fundo.CodigoCVM,
			&fundo.DataRegistro,
			&fundo.DataConstituicao,
			&fundo.TipoFundo,
			&fundo.DenominacaoSocial,
			&fundo.DataCancelamento,
			&fundo.Situacao,
			&fundo.DataInicioSituacao,
			&fundo.DataAdaptacaoRCVM175,
			&fundo.DataInicioExercicio,
			&fundo.DataFimExercicio,
			&fundo.PatrimonioLiquido,
			&fundo.DataPatrimonioLiquido,
			&fundo.Diretor,
			&fundo.CNPJAdministrador,
			&fundo.Administrador,
			&fundo.TipoPessoaGestor,
			&fundo.CPFCNPJGestor,
			&fundo.Gestor,
		)
		if err != nil {
			http.Error(w, "Erro ao fazer scan dos dados: "+err.Error(), http.StatusInternalServerError)
			return
		}
		fundos = append(fundos, fundo)
	}

	if err = rows.Err(); err != nil {
		http.Error(w, "Erro durante iteração das linhas: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Configura o header para JSON
	w.Header().Set("Content-Type", "application/json")

	// Codifica e retorna o JSON
	if err := json.NewEncoder(w).Encode(fundos); err != nil {
		http.Error(w, "Erro ao codificar JSON: "+err.Error(), http.StatusInternalServerError)
		return
	}
}

func admFiiHandler(w http.ResponseWriter, r *http.Request) {
	// Conecta ao banco
	db, err := conectaDB()
	if err != nil {
		http.Error(w, "Erro ao conectar ao banco de dados: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer db.Close()

	// Executa a consulta
	rows, err := executarConsulta(db, "sql/adm_fii.sql")
	if err != nil {
		http.Error(w, "Erro ao executar consulta: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	// Processa os resultados
	var admFiis []models.AdmFii
	for rows.Next() {
		var admFii models.AdmFii
		err = rows.Scan(
			&admFii.TpFundoClasse,
			&admFii.CnpjFundoClasse,
			&admFii.DenomSocial,
			&admFii.DtReg,
			&admFii.DtConst,
			&admFii.CdCvm,
			&admFii.DtCancel,
			&admFii.Sit,
			&admFii.DtIniSit,
			&admFii.DtIniAtiv,
			&admFii.DtIniExerc,
			&admFii.DtFimExerc,
			&admFii.Classe,
			&admFii.DtIniClasse,
			&admFii.RentabFundo,
			&admFii.Condom,
			&admFii.FundoCotas,
			&admFii.FundoExclusivo,
			&admFii.TribLprazo,
			&admFii.PublicoAlvo,
			&admFii.EntidInvest,
			&admFii.TaxaPerfm,
			&admFii.InfTaxaPerfm,
			&admFii.TaxaAdm,
			&admFii.InfTaxaAdm,
			&admFii.VlPatrimLiq,
			&admFii.DtPatrimLiq,
			&admFii.Diretor,
			&admFii.CnpjAdmin,
			&admFii.Admin,
			&admFii.PfPjGestor,
			&admFii.CpfCnpjGestor,
			&admFii.Gestor,
			&admFii.CnpjAuditor,
			&admFii.Auditor,
			&admFii.CnpjCustodiante,
			&admFii.Custodiante,
			&admFii.CnpjControlador,
			&admFii.Controlador,
			&admFii.InvestCemprExter,
			&admFii.ClasseAnbima,
		)
		if err != nil {
			http.Error(w, "Erro ao fazer scan dos dados: "+err.Error(), http.StatusInternalServerError)
			return
		}

		admFiis = append(admFiis, admFii)
	}

	if err = rows.Err(); err != nil {
		http.Error(w, "Erro durante iteração das linhas: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Configura o header para JSON
	w.Header().Set("Content-Type", "application/json")

	// Codifica e retorna o JSON
	if err := json.NewEncoder(w).Encode(admFiis); err != nil {
		http.Error(w, "Erro ao codificar JSON: "+err.Error(), http.StatusInternalServerError)
		return
	}
}
