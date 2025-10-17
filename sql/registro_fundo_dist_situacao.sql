SELECT situacao, COUNT(*) as quantidade
FROM registro_fundo 
GROUP BY situacao
