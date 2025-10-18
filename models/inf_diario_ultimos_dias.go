package models

type InformeDiarioLastDays struct {
	TpFundoClasse   *string `json:"tp_fundo_classe"`
	CNPJFundoClasse *string `json:"cnpj_fundo_classe"`
	IdSubclasse     *string `json:"id_subclasse"`
	DtComptc        *string `json:"dt_comptc"`
	VlTotal         *string `json:"vl_total"`
	VlQuota         *string `json:"vl_quota"`
	VlPatrimLiq     *string `json:"vl_patrim_liq"`
	CaptcDia        *string `json:"captc_dia"`
	ResgDia         *string `json:"resg_dia"`
	NrCotst         *string `json:"nr_cotst"`
	Dia             *string `json:"dia"`
	Mes             *string `json:"mes"`
	Ano             *string `json:"ano"`
}
