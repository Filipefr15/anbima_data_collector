select denominacao_social from registro_fundo
where cnpj_fundo = :cnpj_fundo
order by data_registro desc
limit 1