select 
inf.tp_fundo_classe, cad.denom_social, inf.cnpj_fundo_classe, inf.dt_comptc, inf.vl_total, inf.vl_quota, inf.vl_patrim_liq,
inf.captc_dia, inf.resg_dia, inf.nr_cotst, cad.dt_reg, cad.dt_const, cad.cd_cvm, cad.sit, cad.dt_ini_sit, cad.dt_ini_ativ,
cad.dt_ini_exerc, cad.dt_fim_exerc, cad.classe, cad.dt_ini_classe, cad.rentab_fundo as benchmark, cad.condom as condominio,
cad.fundo_cotas, cad.fundo_exclusivo, cad.trib_lprazo, cad.publico_alvo, cad.entid_invest, cad.taxa_perfm, cad.inf_taxa_perfm,
cad.taxa_adm, cad.inf_taxa_adm, cad.diretor, cad.cnpj_admin, cad.admin, cad.pf_pj_gestor, cad.cpf_cnpj_gestor, cad.gestor,
cad.cnpj_auditor, cad.auditor, cad.cnpj_custodiante, cad.custodiante, cad.cnpj_controlador, cad.controlador, cad.invest_cempr_exter,
cad.classe_anbima
from inf_diario_ultimos_dias as inf
inner join cadastro_fi as cad
ON inf.cnpj_fundo_classe = cad.cnpj_fundo_classe 
where cad.sit != 'CANCELADA' and inf.cnpj_fundo_classe = '49.506.998/0001-90'
order by inf.dt_comptc desc