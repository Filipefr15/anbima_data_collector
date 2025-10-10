select inf.tp_fundo_classe, rf.denominacao_social, inf.cnpj_fundo_classe, inf.dt_comptc, inf.vl_total, inf.vl_quota, inf.vl_patrim_liq,
inf.captc_dia, inf.resg_dia, inf.nr_cotst, rf.codigo_cvm, rf.data_registro, rf.data_constituicao, rf.data_cancelamento, rf.situacao,
rf.data_inicio_situacao, rf.data_adaptacao_rcvm175, rf.data_inicio_exercicio_social, rf.data_fim_exercicio_social, rf.diretor,
rf.cnpj_administrador, rf.administrador, rf.tipo_pessoa_gestor, rf.cpf_cnpj_gestor, rf.gestor 
from inf_diario_ultimos_dias as inf
inner join registro_fundo as rf ON inf.cnpj_fundo_classe = rf.cnpj_fundo
where inf.cnpj_fundo_classe = '04.299.355/0001-84'
--where rf.situacao = 'Em Funcionamento Normal' --and inf.cnpj_fundo_classe = '00.000.432/0001-00'
order by inf.dt_comptc desc

select inf.tp_fundo_classe, rf.denominacao_social, inf.cnpj_fundo_classe, inf.dt_comptc, inf.vl_total, inf.vl_quota, inf.vl_patrim_liq,
inf.captc_dia, inf.resg_dia, inf.nr_cotst, rf.codigo_cvm, rf.data_registro, rf.data_constituicao, rf.data_cancelamento, rf.situacao,
rf.data_inicio_situacao, rf.data_adaptacao_rcvm175, rf.data_inicio_exercicio_social, rf.data_fim_exercicio_social, rf.diretor,
rf.cnpj_administrador, rf.administrador, rf.tipo_pessoa_gestor, rf.cpf_cnpj_gestor, rf.gestor 
from inf_diario_ultimos_dias as inf
inner join registro_fundo as rf ON inf.cnpj_fundo_classe = rf.cnpj_fundo
where inf.cnpj_fundo_classe = '00.068.305/0001-35'
and rf.situacao != 'Cancelado' --and inf.cnpj_fundo_classe = '00.000.432/0001-00'
order by inf.dt_comptc desc
