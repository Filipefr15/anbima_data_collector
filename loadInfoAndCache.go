package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
)

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
	isLatest := r.URL.Query().Get("isLatest") == "true"

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
	if isLatest && len(results) > 0 {
		maxDate := results[0].Data
		for _, info := range results[1:] {
			if info.Data > maxDate {
				maxDate = info.Data
			}
		}
		var filtered []InfoDiario
		for _, info := range results {
			if info.Data == maxDate {
				filtered = append(filtered, info)
			}
		}
		results = filtered
	}

	w.Header().Set("Content-Type", "application/json")
	if !loaded || len(results) == 0 {
		w.Write([]byte("[]"))
		return
	}
	enc := json.NewEncoder(w)
	enc.Encode(results)
}

type InfoFIDCPL struct {
	CNPJ        string `json:"cnpj"`
	DenomSocial string `json:"denom_social"`
	Data        string `json:"data"`
	VlPL        string `json:"vl_pl"`
	VlPLMedio   string `json:"vl_pl_medio"`
	TipoFundo   string `json:"tp_fundo_classe"`
}

type InfoFIDCCOTISTAS struct {
	CNPJ        string `json:"cnpj"`
	DenomSocial string `json:"denom_social"`
	Data        string `json:"data"`
	ClasseSerie string `json:"classe_serie"`
	NumCotst    string `json:"num_cotst"`
	TipoFundo   string `json:"tp_fundo_classe"`
}

type InfoFIDCCOTA struct {
	CNPJ        string `json:"cnpj"`
	DenomSocial string `json:"denom_social"`
	Data        string `json:"data"`
	ClasseSerie string `json:"classe_serie"`
	QtCota      string `json:"qt_cota"`
	VlCota      string `json:"vl_cota"`
	TipoFundo   string `json:"tp_fundo_classe"`
}

type InfoFIDCRENT struct {
	CNPJ        string `json:"cnpj"`
	DenomSocial string `json:"denom_social"`
	Data        string `json:"data"`
	ClasseSerie string `json:"classe_serie"`
	VlRentabMes string `json:"vl_rentab_mes"`
	TipoFundo   string `json:"tp_fundo_classe"`
}

var fidcPLCache map[string][]InfoFIDCPL
var fidcCOTISTASCache map[string][]InfoFIDCCOTISTAS
var fidcCOTACache map[string][]InfoFIDCCOTA
var fidcRENTCache map[string][]InfoFIDCRENT
var fidcCacheLoaded bool
var fidcCacheMutex sync.RWMutex

func loadFIDCCache() error {
	fidcPLCache = make(map[string][]InfoFIDCPL)
	fidcCOTISTASCache = make(map[string][]InfoFIDCCOTISTAS)
	fidcCOTACache = make(map[string][]InfoFIDCCOTA)
	fidcRENTCache = make(map[string][]InfoFIDCRENT)

	// PL
	filePL := "fidcs_anualizados_juntados/fidc_consolidado_IV_.csv"
	fPL, err := os.Open(filePL)
	if err == nil {
		reader := csv.NewReader(fPL)
		reader.FieldsPerRecord = -1
		records, err := reader.ReadAll()
		fPL.Close()
		if err == nil {
			for i, row := range records {
				if i == 0 || len(row) < 6 {
					continue
				}
				info := InfoFIDCPL{
					CNPJ:        strings.TrimSpace(row[0]),
					DenomSocial: row[1],
					Data:        row[2],
					VlPL:        row[3],
					VlPLMedio:   row[4],
					TipoFundo:   row[5],
				}
				key := normalizeCNPJ(info.CNPJ)
				fidcPLCache[key] = append(fidcPLCache[key], info)
			}
		}
	}

	// COTISTAS
	fileCOTISTAS := "fidcs_anualizados_juntados/fidc_consolidado_X_1_.csv"
	fCOTISTAS, err := os.Open(fileCOTISTAS)
	if err == nil {
		reader := csv.NewReader(fCOTISTAS)
		reader.FieldsPerRecord = -1
		records, err := reader.ReadAll()
		fCOTISTAS.Close()
		if err == nil {
			for i, row := range records {
				if i == 0 || len(row) < 6 {
					continue
				}
				info := InfoFIDCCOTISTAS{
					CNPJ:        strings.TrimSpace(row[0]),
					DenomSocial: row[1],
					Data:        row[2],
					ClasseSerie: row[3],
					NumCotst:    row[4],
					TipoFundo:   row[5],
				}
				key := normalizeCNPJ(info.CNPJ)
				fidcCOTISTASCache[key] = append(fidcCOTISTASCache[key], info)
			}
		}
	}

	// COTA
	fileCOTA := "fidcs_anualizados_juntados/fidc_consolidado_X_2_.csv"
	fCOTA, err := os.Open(fileCOTA)
	if err == nil {
		reader := csv.NewReader(fCOTA)
		reader.FieldsPerRecord = -1
		records, err := reader.ReadAll()
		fCOTA.Close()
		if err == nil {
			for i, row := range records {
				if i == 0 || len(row) < 7 {
					continue
				}
				info := InfoFIDCCOTA{
					CNPJ:        strings.TrimSpace(row[0]),
					DenomSocial: row[1],
					Data:        row[2],
					ClasseSerie: row[3],
					QtCota:      row[4],
					VlCota:      row[5],
					TipoFundo:   row[6],
				}
				key := normalizeCNPJ(info.CNPJ)
				fidcCOTACache[key] = append(fidcCOTACache[key], info)
			}
		}
	}

	// RENT
	fileRENT := "fidcs_anualizados_juntados/fidc_consolidado_X_3_.csv"
	fRENT, err := os.Open(fileRENT)
	if err == nil {
		reader := csv.NewReader(fRENT)
		reader.FieldsPerRecord = -1
		records, err := reader.ReadAll()
		fRENT.Close()
		if err == nil {
			for i, row := range records {
				if i == 0 || len(row) < 6 {
					continue
				}
				info := InfoFIDCRENT{
					CNPJ:        strings.TrimSpace(row[0]),
					DenomSocial: row[1],
					Data:        row[2],
					ClasseSerie: row[3],
					VlRentabMes: row[4],
					TipoFundo:   row[5],
				}
				key := normalizeCNPJ(info.CNPJ)
				fidcRENTCache[key] = append(fidcRENTCache[key], info)
			}
		}
	}

	fidcCacheMutex.Lock()
	fidcCacheLoaded = true
	fidcCacheMutex.Unlock()
	return nil
}

func searchFIDCHandler(w http.ResponseWriter, r *http.Request) {
	cnpj := r.URL.Query().Get("cnpj")
	if cnpj == "" {
		http.Error(w, "Parâmetro 'cnpj' é obrigatório", http.StatusBadRequest)
		return
	}

	year := r.URL.Query().Get("year")
	month := r.URL.Query().Get("month")
	table := r.URL.Query().Get("table") // "PL", "COTISTAS", "COTA", "RENT" ou vazio para todas
	isLatest := r.URL.Query().Get("isLatest") == "true"

	normCNPJ := normalizeCNPJ(strings.TrimSpace(cnpj))

	fidcCacheMutex.RLock()
	loaded := fidcCacheLoaded
	var resp = make(map[string]interface{})

	if table == "PL" || table == "" {
		var results []InfoFIDCPL
		for _, info := range fidcPLCache[normCNPJ] {
			if year != "" && (len(info.Data) < 4 || info.Data[:4] != year) {
				continue
			}
			if month != "" && (len(info.Data) < 7 || info.Data[5:7] != month) {
				continue
			}
			results = append(results, info)
		}
		if isLatest && len(results) > 0 {
			maxDate := results[0].Data
			for _, info := range results[1:] {
				if info.Data > maxDate {
					maxDate = info.Data
				}
			}
			var filtered []InfoFIDCPL
			for _, info := range results {
				if info.Data == maxDate {
					filtered = append(filtered, info)
				}
			}
			results = filtered
		}
		if table == "PL" {
			fidcCacheMutex.RUnlock()
			w.Header().Set("Content-Type", "application/json")
			if !loaded || len(results) == 0 {
				w.Write([]byte("[]"))
				return
			}
			enc := json.NewEncoder(w)
			enc.Encode(results)
			return
		}
		resp["PL"] = results
	}
	if table == "COTISTAS" || table == "" {
		var results []InfoFIDCCOTISTAS
		for _, info := range fidcCOTISTASCache[normCNPJ] {
			if year != "" && (len(info.Data) < 4 || info.Data[:4] != year) {
				continue
			}
			if month != "" && (len(info.Data) < 7 || info.Data[5:7] != month) {
				continue
			}
			results = append(results, info)
		}
		if isLatest && len(results) > 0 {
			maxDate := results[0].Data
			for _, info := range results[1:] {
				if info.Data > maxDate {
					maxDate = info.Data
				}
			}
			var filtered []InfoFIDCCOTISTAS
			for _, info := range results {
				numCotstInt, err := strconv.Atoi(info.NumCotst)
				if err != nil {
					continue
				}
				if info.Data == maxDate && numCotstInt > 0 {
					filtered = append(filtered, info)
				}
			}
			results = filtered
		}
		if table == "COTISTAS" {
			fidcCacheMutex.RUnlock()
			w.Header().Set("Content-Type", "application/json")
			if !loaded || len(results) == 0 {
				w.Write([]byte("[]"))
				return
			}
			enc := json.NewEncoder(w)
			enc.Encode(results)
			return
		}
		resp["COTISTAS"] = results
	}
	if table == "COTA" || table == "" {
		var results []InfoFIDCCOTA
		for _, info := range fidcCOTACache[normCNPJ] {
			if year != "" && (len(info.Data) < 4 || info.Data[:4] != year) {
				continue
			}
			if month != "" && (len(info.Data) < 7 || info.Data[5:7] != month) {
				continue
			}
			results = append(results, info)
		}
		if isLatest && len(results) > 0 {
			maxDate := results[0].Data
			for _, info := range results[1:] {
				if info.Data > maxDate {
					maxDate = info.Data
				}
			}
			var filtered []InfoFIDCCOTA
			for _, info := range results {
				qtCotaFloat, err := strconv.ParseFloat(info.QtCota, 64)
				if err != nil {
					continue
				}
				vlCotaFloat, err := strconv.ParseFloat(info.VlCota, 64)
				if err != nil {
					continue
				}
				if info.Data == maxDate && qtCotaFloat > 0 && vlCotaFloat > 0 {
					filtered = append(filtered, info)
				}
			}
			results = filtered
		}
		if table == "COTA" {
			fidcCacheMutex.RUnlock()
			w.Header().Set("Content-Type", "application/json")
			if !loaded || len(results) == 0 {
				w.Write([]byte("[]"))
				return
			}
			enc := json.NewEncoder(w)
			enc.Encode(results)
			return
		}
		resp["COTA"] = results
	}
	if table == "RENT" || table == "" {
		var results []InfoFIDCRENT
		for _, info := range fidcRENTCache[normCNPJ] {
			if year != "" && (len(info.Data) < 4 || info.Data[:4] != year) {
				continue
			}
			if month != "" && (len(info.Data) < 7 || info.Data[5:7] != month) {
				continue
			}
			results = append(results, info)
		}
		if isLatest && len(results) > 0 {
			maxDate := results[0].Data
			for _, info := range results[1:] {
				if info.Data > maxDate {
					maxDate = info.Data
				}
			}
			var filtered []InfoFIDCRENT
			for _, info := range results {
				rentFloat, err := strconv.ParseFloat(info.VlRentabMes, 64)
				if err != nil {
					continue
				}
				if info.Data == maxDate && rentFloat != 0.000000 {
					filtered = append(filtered, info)
				}
			}
			results = filtered
		}
		if table == "RENT" {
			fidcCacheMutex.RUnlock()
			w.Header().Set("Content-Type", "application/json")
			if !loaded || len(results) == 0 {
				w.Write([]byte("[]"))
				return
			}
			enc := json.NewEncoder(w)
			enc.Encode(results)
			return
		}
		resp["RENT"] = results
	}
	fidcCacheMutex.RUnlock()
	w.Header().Set("Content-Type", "application/json")
	if !loaded || len(resp) == 0 {
		w.Write([]byte("{}"))
		return
	}
	enc := json.NewEncoder(w)
	enc.Encode(resp)
}

// Estruturas para cada tipo de lamina
type LaminaInfo struct {
	CNPJ                      string `json:"cnpj"`
	DenomSocial               string `json:"denom_social"`
	Data                      string `json:"data"`
	NmFantasia                string `json:"nm_fantasia"`
	EnderEletronico           string `json:"ender_eletronico"`
	PublicoAlvo               string `json:"publico_alvo"`
	RestrInvest               string `json:"restr_invest"`
	Objetivo                  string `json:"objetivo"`
	PolitInvest               string `json:"polit_invest"`
	PrPLAtivoExterior         string `json:"pr_pl_ativo_exterior"`
	PrPLAtivoCredPriv         string `json:"pr_pl_ativo_cred_priv"`
	PrPLAlavanc               string `json:"pr_pl_alavanc"`
	PrAtivoEmissor            string `json:"pr_ativo_emissor"`
	DerivProtecaoCarteira     string `json:"deriv_protecao_carteira"`
	RiscoPerda                string `json:"risco_perda"`
	RiscoPerdaNegativo        string `json:"risco_perda_negativo"`
	PrPLAplicMaxFundoUnico    string `json:"pr_pl_aplic_max_fundo_unico"`
	InvestInicialMin          string `json:"invest_inicial_min"`
	InvestAdic                string `json:"invest_adic"`
	ResgateMin                string `json:"resgate_min"`
	HoraAplicResgate          string `json:"hora_aplic_resgate"`
	VlMinPerman               string `json:"vl_min_perman"`
	QtDiaCaren                string `json:"qt_dia_caren"`
	CondicCaren               string `json:"condic_caren"`
	ConversaoCotaCompra       string `json:"conversao_cota_compra"`
	QtDiaConversaoCotaCompra  string `json:"qt_dia_conversao_cota_compra"`
	ConversaoCotaCanc         string `json:"conversao_cota_canc"`
	QtDiaConversaoCotaResgate string `json:"qt_dia_conversao_cota_resgate"`
	TpDiaPagtoResgate         string `json:"tp_dia_pagto_resgate"`
	QtDiaPagtoResgate         string `json:"qt_dia_pagto_resgate"`
	TpTaxaAdm                 string `json:"tp_taxa_adm"`
	TaxaAdm                   string `json:"taxa_adm"`
	TaxaAdmMin                string `json:"taxa_adm_min"`
	TaxaAdmMax                string `json:"taxa_adm_max"`
	TaxaAdmObs                string `json:"taxa_adm_obs"`
	TaxaEntr                  string `json:"taxa_entr"`
	CondicEntr                string `json:"condic_entr"`
	QtDiaSaida                string `json:"qt_dia_saida"`
	TaxaSaida                 string `json:"taxa_saida"`
	CondicSaida               string `json:"condic_saida"`
	TaxaPerfm                 string `json:"taxa_perfm"`
	PrPLDespesa               string `json:"pr_pl_despesa"`
	DtIniDespesa              string `json:"dt_ini_despesa"`
	DtFimDespesa              string `json:"dt_fim_despesa"`
	EnderEletronicoDespesa    string `json:"ender_eletronico_despesa"`
	VlPatrimLiq               string `json:"vl_patrim_liq"`
	ClasseRiscoAdmin          string `json:"classe_risco_admin"`
	PrRentabFundo5Ano         string `json:"pr_rentab_fundo_5ano"`
	IndiceRefer               string `json:"indice_refer"`
	PrVariacaoIndiceRefer5Ano string `json:"pr_variacao_indice_refer_5ano"`
	QtAnoPerda                string `json:"qt_ano_perda"`
	DtIniAtiv5Ano             string `json:"dt_ini_ativ_5ano"`
	AnoSemRentab              string `json:"ano_sem_rentab"`
	CalcRentabFundoGatilho    string `json:"calc_rentab_fundo_gatilho"`
	PrVariacaoPerfm           string `json:"pr_variacao_perfm"`
	CalcRentabFundo           string `json:"calc_rentab_fundo"`
	RentabGatilho             string `json:"rentab_gatilho"`
	DsRentabGatilho           string `json:"ds_rentab_gatilho"`
	AnoExemplo                string `json:"ano_exemplo"`
	AnoAnterExemplo           string `json:"ano_anter_exemplo"`
	VlResgateExemplo          string `json:"vl_resgate_exemplo"`
	VlImpostoExemplo          string `json:"vl_imposto_exemplo"`
	VlTaxaEntrExemplo         string `json:"vl_taxa_entr_exemplo"`
	VlTaxaSaidaExemplo        string `json:"vl_taxa_saida_exemplo"`
	VlAjustePerfmExemplo      string `json:"vl_ajuste_perfm_exemplo"`
	VlDespesaExemplo          string `json:"vl_despesa_exemplo"`
	VlDespesa3Ano             string `json:"vl_despesa_3ano"`
	VlDespesa5Ano             string `json:"vl_despesa_5ano"`
	VlRetorno3Ano             string `json:"vl_retorno_3ano"`
	VlRetorno5Ano             string `json:"vl_retorno_5ano"`
	RemunDistrib              string `json:"remun_distrib"`
	DistribGestorUnico        string `json:"distrib_gestor_unico"`
	ConflitoVenda             string `json:"conflito_venda"`
	TelSAC                    string `json:"tel_sac"`
	EnderEletronicoReclamacao string `json:"ender_eletronico_reclamacao"`
	InfSAC                    string `json:"inf_sac"`
	TpFundoClasse             string `json:"tp_fundo_classe"`
	IdSubclasse               string `json:"id_subclasse"`
}

type LaminaCarteiraInfo struct {
	CNPJ          string `json:"cnpj"`
	DenomSocial   string `json:"denom_social"`
	Data          string `json:"data"`
	TpAtivo       string `json:"tp_ativo"`
	PrPLAtivo     string `json:"pr_pl_ativo"`
	TpFundoClasse string `json:"tp_fundo_classe"`
	IdSubclasse   string `json:"id_subclasse"`
}

type LaminaRentabAnoInfo struct {
	CNPJ                     string `json:"cnpj"`
	DenomSocial              string `json:"denom_social"`
	Data                     string `json:"data"`
	AnoRentab                string `json:"ano_rentab"`
	PrRentabAno              string `json:"pr_rentab_ano"`
	PrVariacaoIndiceReferAno string `json:"pr_variacao_indice_refer_ano"`
	PrPerfmIndiceReferAno    string `json:"pr_perfm_indice_refer_ano"`
	RentabAnoObs             string `json:"rentab_ano_obs"`
	TpFundoClasse            string `json:"tp_fundo_classe"`
	IdSubclasse              string `json:"id_subclasse"`
}

type LaminaRentabMesInfo struct {
	CNPJ                     string `json:"cnpj"`
	DenomSocial              string `json:"denom_social"`
	Data                     string `json:"data"`
	MesRentab                string `json:"mes_rentab"`
	PrRentabMes              string `json:"pr_rentab_mes"`
	PrVariacaoIndiceReferMes string `json:"pr_variacao_indice_refer_mes"`
	PrPerfmIndiceReferMes    string `json:"pr_perfm_indice_refer_mes"`
	RentabMesObs             string `json:"rentab_mes_obs"`
	TpFundoClasse            string `json:"tp_fundo_classe"`
	IdSubclasse              string `json:"id_subclasse"`
}

var laminaCache map[string][]LaminaInfo
var laminaCarteiraCache map[string][]LaminaCarteiraInfo
var laminaRentabAnoCache map[string][]LaminaRentabAnoInfo
var laminaRentabMesCache map[string][]LaminaRentabMesInfo
var laminaCacheLoaded bool
var laminaCacheMutex sync.RWMutex

func loadLaminaCache(anos []int) error {
	laminaCache = make(map[string][]LaminaInfo)
	laminaCarteiraCache = make(map[string][]LaminaCarteiraInfo)
	laminaRentabAnoCache = make(map[string][]LaminaRentabAnoInfo)
	laminaRentabMesCache = make(map[string][]LaminaRentabMesInfo)

	// lamina_final_.csv
	meses := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}
	for _, ano := range anos {
		for _, mes := range meses {
			if f, err := os.Open(fmt.Sprintf("lamina_padronized/lamina_fi_%d%02d.csv", ano, mes)); err == nil {
				reader := csv.NewReader(f)
				reader.FieldsPerRecord = -1
				records, err := reader.ReadAll()
				f.Close()
				if err == nil {
					for i, row := range records {
						if i == 0 || len(row) < 77 {
							continue
						}
						info := LaminaInfo{
							CNPJ: row[0], DenomSocial: row[1], Data: row[2], NmFantasia: row[3], EnderEletronico: row[4], PublicoAlvo: row[5], RestrInvest: row[6], Objetivo: row[7], PolitInvest: row[8], PrPLAtivoExterior: row[9], PrPLAtivoCredPriv: row[10], PrPLAlavanc: row[11], PrAtivoEmissor: row[12], DerivProtecaoCarteira: row[13], RiscoPerda: row[14], RiscoPerdaNegativo: row[15], PrPLAplicMaxFundoUnico: row[16], InvestInicialMin: row[17], InvestAdic: row[18], ResgateMin: row[19], HoraAplicResgate: row[20], VlMinPerman: row[21], QtDiaCaren: row[22], CondicCaren: row[23], ConversaoCotaCompra: row[24], QtDiaConversaoCotaCompra: row[25], ConversaoCotaCanc: row[26], QtDiaConversaoCotaResgate: row[27], TpDiaPagtoResgate: row[28], QtDiaPagtoResgate: row[29], TpTaxaAdm: row[30], TaxaAdm: row[31], TaxaAdmMin: row[32], TaxaAdmMax: row[33], TaxaAdmObs: row[34], TaxaEntr: row[35], CondicEntr: row[36], QtDiaSaida: row[37], TaxaSaida: row[38], CondicSaida: row[39], TaxaPerfm: row[40], PrPLDespesa: row[41], DtIniDespesa: row[42], DtFimDespesa: row[43], EnderEletronicoDespesa: row[44], VlPatrimLiq: row[45], ClasseRiscoAdmin: row[46], PrRentabFundo5Ano: row[47], IndiceRefer: row[48], PrVariacaoIndiceRefer5Ano: row[49], QtAnoPerda: row[50], DtIniAtiv5Ano: row[51], AnoSemRentab: row[52], CalcRentabFundoGatilho: row[53], PrVariacaoPerfm: row[54], CalcRentabFundo: row[55], RentabGatilho: row[56], DsRentabGatilho: row[57], AnoExemplo: row[58], AnoAnterExemplo: row[59], VlResgateExemplo: row[60], VlImpostoExemplo: row[61], VlTaxaEntrExemplo: row[62], VlTaxaSaidaExemplo: row[63], VlAjustePerfmExemplo: row[64], VlDespesaExemplo: row[65], VlDespesa3Ano: row[66], VlDespesa5Ano: row[67], VlRetorno3Ano: row[68], VlRetorno5Ano: row[69], RemunDistrib: row[70], DistribGestorUnico: row[71], ConflitoVenda: row[72], TelSAC: row[73], EnderEletronicoReclamacao: row[74], InfSAC: row[75], TpFundoClasse: row[76], IdSubclasse: row[77],
						}
						key := strings.ReplaceAll(row[0], ".", "")
						key = strings.ReplaceAll(key, "-", "")
						key = strings.ReplaceAll(key, "/", "")
						laminaCache[key] = append(laminaCache[key], info)
					}
				}
			}
		}
	}

	// lamina_final_carteira_.csv
	for _, ano := range anos {
		for _, mes := range meses {
			if f, err := os.Open(fmt.Sprintf("lamina_padronized/lamina_fi_carteira_%d%02d.csv", ano, mes)); err == nil {
				reader := csv.NewReader(f)
				reader.FieldsPerRecord = -1
				records, err := reader.ReadAll()
				f.Close()
				if err == nil {
					for i, row := range records {
						if i == 0 || len(row) < 7 {
							continue
						}
						info := LaminaCarteiraInfo{
							CNPJ: row[0], DenomSocial: row[1], Data: row[2], TpAtivo: row[3], PrPLAtivo: row[4], TpFundoClasse: row[5], IdSubclasse: row[6],
						}
						key := strings.ReplaceAll(row[0], ".", "")
						key = strings.ReplaceAll(key, "-", "")
						key = strings.ReplaceAll(key, "/", "")
						laminaCarteiraCache[key] = append(laminaCarteiraCache[key], info)
					}
				}
			}
		}
	}
	// lamina_final_rentab_ano_.csv
	for _, ano := range anos {
		for _, mes := range meses {
			if f, err := os.Open(fmt.Sprintf("lamina_padronized/lamina_fi_rentab_ano_%d%02d.csv", ano, mes)); err == nil {
				reader := csv.NewReader(f)
				reader.FieldsPerRecord = -1
				records, err := reader.ReadAll()
				f.Close()
				if err == nil {
					for i, row := range records {
						if i == 0 || len(row) < 10 {
							continue
						}
						info := LaminaRentabAnoInfo{
							CNPJ: row[0], DenomSocial: row[1], Data: row[2], AnoRentab: row[3], PrRentabAno: row[4], PrVariacaoIndiceReferAno: row[5], PrPerfmIndiceReferAno: row[6], RentabAnoObs: row[7], TpFundoClasse: row[8], IdSubclasse: row[9],
						}
						key := strings.ReplaceAll(row[0], ".", "")
						key = strings.ReplaceAll(key, "-", "")
						key = strings.ReplaceAll(key, "/", "")
						laminaRentabAnoCache[key] = append(laminaRentabAnoCache[key], info)
					}
				}
			}
		}
	}

	// lamina_final_rentab_mes_.csv
	for _, ano := range anos {
		for _, mes := range meses {
			if f, err := os.Open(fmt.Sprintf("lamina_padronized/lamina_fi_rentab_mes_%d%02d.csv", ano, mes)); err == nil {
				reader := csv.NewReader(f)
				reader.FieldsPerRecord = -1
				records, err := reader.ReadAll()
				f.Close()
				if err == nil {
					for i, row := range records {
						if i == 0 || len(row) < 10 {
							continue
						}
						info := LaminaRentabMesInfo{
							CNPJ: row[0], DenomSocial: row[1], Data: row[2], MesRentab: row[3], PrRentabMes: row[4], PrVariacaoIndiceReferMes: row[5], PrPerfmIndiceReferMes: row[6], RentabMesObs: row[7], TpFundoClasse: row[8], IdSubclasse: row[9],
						}
						key := strings.ReplaceAll(row[0], ".", "")
						key = strings.ReplaceAll(key, "-", "")
						key = strings.ReplaceAll(key, "/", "")
						laminaRentabMesCache[key] = append(laminaRentabMesCache[key], info)
					}
				}
			}
		}
	}

	laminaCacheMutex.Lock()
	laminaCacheLoaded = true
	laminaCacheMutex.Unlock()
	return nil
}

// Handler para busca
func searchLaminaHandler(w http.ResponseWriter, r *http.Request) {
	cnpj := r.URL.Query().Get("cnpj")
	if cnpj == "" {
		http.Error(w, "Parâmetro 'cnpj' é obrigatório", http.StatusBadRequest)
		return
	}
	table := strings.ToUpper(r.URL.Query().Get("table")) // "LAMINA", "CARTEIRA", "RENTAB_ANO", "RENTAB_MES" ou vazio para todas
	year := r.URL.Query().Get("year")
	month := r.URL.Query().Get("month")
	isLatest := r.URL.Query().Get("isLatest") == "true"

	normCNPJ := normalizeCNPJ(strings.TrimSpace(cnpj))

	laminaCacheMutex.RLock()
	loaded := laminaCacheLoaded
	resp := make(map[string]interface{})

	if table == "LAMINA" || table == "" {
		var results []LaminaInfo
		for _, info := range laminaCache[normCNPJ] {
			if year != "" && (len(info.Data) < 4 || info.Data[:4] != year) {
				continue
			}
			if month != "" && (len(info.Data) < 7 || info.Data[5:7] != month) {
				continue
			}
			results = append(results, info)
		}
		if isLatest && len(results) > 0 {
			maxDate := results[0].Data
			for _, info := range results[1:] {
				if info.Data > maxDate {
					maxDate = info.Data
				}
			}
			var filtered []LaminaInfo
			for _, info := range results {
				if info.Data == maxDate {
					filtered = append(filtered, info)
				}
			}
			results = filtered
		}
		if table == "LAMINA" {
			laminaCacheMutex.RUnlock()
			w.Header().Set("Content-Type", "application/json")
			if !loaded || len(results) == 0 {
				w.Write([]byte("[]"))
				return
			}
			enc := json.NewEncoder(w)
			enc.Encode(results)
			return
		}
		resp["LAMINA"] = results
	}
	if table == "CARTEIRA" || table == "" {
		var results []LaminaCarteiraInfo
		for _, info := range laminaCarteiraCache[normCNPJ] {
			if year != "" && (len(info.Data) < 4 || info.Data[:4] != year) {
				continue
			}
			if month != "" && (len(info.Data) < 7 || info.Data[5:7] != month) {
				continue
			}
			results = append(results, info)
		}
		if isLatest && len(results) > 0 {
			maxDate := results[0].Data
			for _, info := range results[1:] {
				if info.Data > maxDate {
					maxDate = info.Data
				}
			}
			var filtered []LaminaCarteiraInfo
			for _, info := range results {
				if info.Data == maxDate {
					filtered = append(filtered, info)
				}
			}
			results = filtered
		}
		if table == "CARTEIRA" {
			laminaCacheMutex.RUnlock()
			w.Header().Set("Content-Type", "application/json")
			if !loaded || len(results) == 0 {
				w.Write([]byte("[]"))
				return
			}
			enc := json.NewEncoder(w)
			enc.Encode(results)
			return
		}
		resp["CARTEIRA"] = results
	}
	if table == "RENTAB_ANO" || table == "" {
		var results []LaminaRentabAnoInfo
		for _, info := range laminaRentabAnoCache[normCNPJ] {
			if year != "" && info.AnoRentab != year {
				continue
			}
			if month != "" && (len(info.Data) < 7 || info.Data[5:7] != month) {
				continue
			}
			results = append(results, info)
		}
		if isLatest && len(results) > 0 {
			maxDate := results[0].Data
			for _, info := range results[1:] {
				if info.Data > maxDate {
					maxDate = info.Data
				}
			}
			var filtered []LaminaRentabAnoInfo
			for _, info := range results {
				if info.Data == maxDate {
					filtered = append(filtered, info)
				}
			}
			results = filtered
		}
		if table == "RENTAB_ANO" {
			laminaCacheMutex.RUnlock()
			w.Header().Set("Content-Type", "application/json")
			if !loaded || len(results) == 0 {
				w.Write([]byte("[]"))
				return
			}
			enc := json.NewEncoder(w)
			enc.Encode(results)
			return
		}
		resp["RENTAB_ANO"] = results
	}
	if table == "RENTAB_MES" || table == "" {
		var results []LaminaRentabMesInfo
		for _, info := range laminaRentabMesCache[normCNPJ] {
			if year != "" && (len(info.Data) < 4 || info.Data[:4] != year) {
				continue
			}
			mesRentabInt, _ := strconv.Atoi(info.MesRentab)
			monthInt, _ := strconv.Atoi(month)
			if month != "" && mesRentabInt != monthInt {
				continue
			}
			results = append(results, info)
		}
		if isLatest && len(results) > 0 {
			maxDate := results[0].Data
			for _, info := range results[1:] {
				if info.Data > maxDate {
					maxDate = info.Data
				}
			}
			var filtered []LaminaRentabMesInfo
			for _, info := range results {
				if info.Data == maxDate {
					filtered = append(filtered, info)
				}
			}
			results = filtered
		}
		if table == "RENTAB_MES" {
			laminaCacheMutex.RUnlock()
			w.Header().Set("Content-Type", "application/json")
			if !loaded || len(results) == 0 {
				w.Write([]byte("[]"))
				return
			}
			enc := json.NewEncoder(w)
			enc.Encode(results)
			return
		}
		resp["RENTAB_MES"] = results
	}
	laminaCacheMutex.RUnlock()
	w.Header().Set("Content-Type", "application/json")
	if !loaded || len(resp) == 0 {
		w.Write([]byte("{}"))
		return
	}
	enc := json.NewEncoder(w)
	enc.Encode(resp)
}

// Estrutura para FIP
type InfoFIP struct {
	CNPJ                          string `json:"cnpj"`
	DenomSocial                   string `json:"denom_social"`
	Data                          string `json:"data"`
	VlPatrimLiq                   string `json:"vl_patrim_liq"`
	QtCota                        string `json:"qt_cota"`
	VlPatrimCota                  string `json:"vl_patrim_cota"`
	NrCotst                       string `json:"nr_cotst"`
	EntidInvest                   string `json:"entid_invest"`
	PublicoAlvo                   string `json:"publico_alvo"`
	VlCapComprom                  string `json:"vl_cap_comprom"`
	QtCotaSubscr                  string `json:"qt_cota_subscr"`
	VlCapSubscr                   string `json:"vl_cap_subscr"`
	QtCotaIntegr                  string `json:"qt_cota_integr"`
	VlCapIntegr                   string `json:"vl_cap_integr"`
	VlInvestFipCota               string `json:"vl_invest_fip_cota"`
	NrCotstSubscrPF               string `json:"nr_cotst_subscr_pf"`
	PrCotaSubscrPF                string `json:"pr_cota_subscr_pf"`
	NrCotstSubscrPJNF             string `json:"nr_cotst_subscr_pj_nao_financ"`
	PrCotaSubscrPJNF              string `json:"pr_cota_subscr_pj_nao_financ"`
	NrCotstSubscrBanco            string `json:"nr_cotst_subscr_banco"`
	PrCotaSubscrBanco             string `json:"pr_cota_subscr_banco"`
	NrCotstSubscrCorretoraDistrib string `json:"nr_cotst_subscr_corretora_distrib"`
	PrCotaSubscrCorretoraDistrib  string `json:"pr_cota_subscr_corretora_distrib"`
	NrCotstSubscrPJFinanc         string `json:"nr_cotst_subscr_pj_financ"`
	PrCotaSubscrPJFinanc          string `json:"pr_cota_subscr_pj_financ"`
	NrCotstSubscrINVNR            string `json:"nr_cotst_subscr_invnr"`
	PrCotaSubscrINVNR             string `json:"pr_cota_subscr_invnr"`
	NrCotstSubscrEAPC             string `json:"nr_cotst_subscr_eapc"`
	PrCotaSubscrEAPC              string `json:"pr_cota_subscr_eapc"`
	NrCotstSubscrEFPC             string `json:"nr_cotst_subscr_efpc"`
	PrCotaSubscrEFPC              string `json:"pr_cota_subscr_efpc"`
	NrCotstSubscrRPPS             string `json:"nr_cotst_subscr_rpps"`
	PrCotaSubscrRPPS              string `json:"pr_cota_subscr_rpps"`
	NrCotstSubscrSegur            string `json:"nr_cotst_subscr_segur"`
	PrCotaSubscrSegur             string `json:"pr_cota_subscr_segur"`
	NrCotstSubscrCapitaliz        string `json:"nr_cotst_subscr_capitaliz"`
	PrCotaSubscrCapitaliz         string `json:"pr_cota_subscr_capitaliz"`
	NrCotstSubscrFII              string `json:"nr_cotst_subscr_fii"`
	PrCotaSubscrFII               string `json:"pr_cota_subscr_fii"`
	NrCotstSubscrFI               string `json:"nr_cotst_subscr_fi"`
	PrCotaSubscrFI                string `json:"pr_cota_subscr_fi"`
	NrCotstSubscrDistrib          string `json:"nr_cotst_subscr_distrib"`
	PrCotaSubscrDistrib           string `json:"pr_cota_subscr_distrib"`
	NrCotstSubscrOutro            string `json:"nr_cotst_subscr_outro"`
	PrCotaSubscrOutro             string `json:"pr_cota_subscr_outro"`
	NrTotalCotstSubscr            string `json:"nr_total_cotst_subscr"`
	PrTotalCotaSubscr             string `json:"pr_total_cota_subscr"`
	ClasseCota                    string `json:"classe_cota"`
	NrCotstSubscrClasse           string `json:"nr_cotst_subscr_classe"`
	QtCotaSubscrClasse            string `json:"qt_cota_subscr_classe"`
	QtCotaIntegrClasse            string `json:"qt_cota_integr_classe"`
	VlQuotaClasse                 string `json:"vl_quota_classe"`
	DireitoPolitClasse            string `json:"direito_polit_classe"`
	DireitoEconClasse             string `json:"direito_econ_classe"`
	TpFundoClasse                 string `json:"tp_fundo_classe"`
}

var fipCache map[string][]InfoFIP
var fipCacheLoaded bool
var fipCacheMutex sync.RWMutex

func loadFipCache() error {
	fipCache = make(map[string][]InfoFIP)
	file := "fip_final/inf_tri_quadri_fip_geral.csv"
	f, err := os.Open(file)
	if err != nil {
		return err
	}
	defer f.Close()
	reader := csv.NewReader(f)
	reader.Comma = ','
	reader.FieldsPerRecord = -1
	records, err := reader.ReadAll()
	if err != nil {
		return err
	}
	for i, row := range records {
		if i == 0 || len(row) < 55 {
			continue
		}
		info := InfoFIP{
			CNPJ: row[0], DenomSocial: row[1], Data: row[2], VlPatrimLiq: row[3], QtCota: row[4],
			VlPatrimCota: row[5], NrCotst: row[6], EntidInvest: row[7], PublicoAlvo: row[8], VlCapComprom: row[9],
			QtCotaSubscr: row[10], VlCapSubscr: row[11], QtCotaIntegr: row[12], VlCapIntegr: row[13], VlInvestFipCota: row[14],
			NrCotstSubscrPF: row[15], PrCotaSubscrPF: row[16], NrCotstSubscrPJNF: row[17], PrCotaSubscrPJNF: row[18], NrCotstSubscrBanco: row[19],
			PrCotaSubscrBanco: row[20], NrCotstSubscrCorretoraDistrib: row[21], PrCotaSubscrCorretoraDistrib: row[22], NrCotstSubscrPJFinanc: row[23], PrCotaSubscrPJFinanc: row[24],
			NrCotstSubscrINVNR: row[25], PrCotaSubscrINVNR: row[26], NrCotstSubscrEAPC: row[27], PrCotaSubscrEAPC: row[28], NrCotstSubscrEFPC: row[29],
			PrCotaSubscrEFPC: row[30], NrCotstSubscrRPPS: row[31], PrCotaSubscrRPPS: row[32], NrCotstSubscrSegur: row[33], PrCotaSubscrSegur: row[34],
			NrCotstSubscrCapitaliz: row[35], PrCotaSubscrCapitaliz: row[36], NrCotstSubscrFII: row[37], PrCotaSubscrFII: row[38], NrCotstSubscrFI: row[39],
			PrCotaSubscrFI: row[40], NrCotstSubscrDistrib: row[41], PrCotaSubscrDistrib: row[42], NrCotstSubscrOutro: row[43], PrCotaSubscrOutro: row[44],
			NrTotalCotstSubscr: row[45], PrTotalCotaSubscr: row[46], ClasseCota: row[47], NrCotstSubscrClasse: row[48], QtCotaSubscrClasse: row[49],
			QtCotaIntegrClasse: row[50], VlQuotaClasse: row[51], DireitoPolitClasse: row[52], DireitoEconClasse: row[53], TpFundoClasse: row[54],
		}
		key := normalizeCNPJ(strings.TrimSpace(row[0]))
		fipCache[key] = append(fipCache[key], info)
	}
	fipCacheMutex.Lock()
	fipCacheLoaded = true
	fipCacheMutex.Unlock()
	return nil
}

func searchFipHandler(w http.ResponseWriter, r *http.Request) {
	cnpj := r.URL.Query().Get("cnpj")
	if cnpj == "" {
		http.Error(w, "Parâmetro 'cnpj' é obrigatório", http.StatusBadRequest)
		return
	}
	year := r.URL.Query().Get("year")
	month := r.URL.Query().Get("month")
	isLatest := r.URL.Query().Get("isLatest") == "true"

	normCNPJ := normalizeCNPJ(strings.TrimSpace(cnpj))

	fipCacheMutex.RLock()
	loaded := fipCacheLoaded
	allResults := fipCache[normCNPJ]
	fipCacheMutex.RUnlock()

	var results []InfoFIP
	for _, info := range allResults {
		if year != "" && (len(info.Data) < 4 || info.Data[:4] != year) {
			continue
		}
		if month != "" && (len(info.Data) < 7 || info.Data[5:7] != month) {
			continue
		}
		results = append(results, info)
	}
	if isLatest && len(results) > 0 {
		maxDate := results[0].Data
		for _, info := range results[1:] {
			if info.Data > maxDate {
				maxDate = info.Data
			}
		}
		var filtered []InfoFIP
		for _, info := range results {
			if info.Data == maxDate {
				filtered = append(filtered, info)
			}
		}
		results = filtered
	}

	w.Header().Set("Content-Type", "application/json")
	if !loaded || len(results) == 0 {
		w.Write([]byte("[]"))
		return
	}
	enc := json.NewEncoder(w)
	enc.Encode(results)
}
