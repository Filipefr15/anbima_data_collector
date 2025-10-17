SELECT denominacao_social, patrimonio_liquido
FROM registro_fundo 
WHERE patrimonio_liquido IS NOT NULL
and patrimonio_liquido < 1700000000000
ORDER BY patrimonio_liquido DESC 
LIMIT 10