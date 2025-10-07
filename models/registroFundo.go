package models

type RegistroFundo struct {
	IdRegistroFundo       *string `json:"id_registro_fundo"`
	CNPJFundo             *string `json:"cnpj_fundo"`
	CodigoCVM             *string `json:"codigo_cvm"`
	DataRegistro          *string `json:"data_registro"`
	DataConstituicao      *string `json:"data_constituicao"`
	TipoFundo             *string `json:"tipo_fundo"`
	DenominacaoSocial     *string `json:"denominacao_social"`
	DataCancelamento      *string `json:"data_cancelamento"`
	Situacao              *string `json:"situacao"`
	DataInicioSituacao    *string `json:"data_inicio_situacao"`
	DataAdaptacaoRCVM175  *string `json:"data_adaptacao_rcvm175"`
	DataInicioExercicio   *string `json:"data_inicio_exercicio_social"`
	DataFimExercicio      *string `json:"data_fim_exercicio_social"`
	PatrimonioLiquido     *string `json:"patrimonio_liquido"`
	DataPatrimonioLiquido *string `json:"data_patrimonio_liquido"`
	Diretor               *string `json:"diretor"`
	CNPJAdministrador     *string `json:"cnpj_administrador"`
	Administrador         *string `json:"administrador"`
	TipoPessoaGestor      *string `json:"tipo_pessoa_gestor"`
	CPFCNPJGestor         *string `json:"cpf_cnpj_gestor"`
	Gestor                *string `json:"gestor"`
}
