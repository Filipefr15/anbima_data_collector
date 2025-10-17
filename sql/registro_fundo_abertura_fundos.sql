SELECT 
    EXTRACT(YEAR FROM data_registro) as ano,
    COUNT(*) as quantidade
FROM registro_fundo 
WHERE data_registro IS NOT NULL
GROUP BY EXTRACT(YEAR FROM data_registro)
ORDER BY ano ASC;