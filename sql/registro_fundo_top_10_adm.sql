SELECT administrador, COUNT(*) as quantidade_fundos
FROM registro_fundo 
WHERE administrador IS NOT NULL
AND situacao = 'Em Funcionamento Normal'
GROUP BY administrador
ORDER BY quantidade_fundos DESC
LIMIT 10