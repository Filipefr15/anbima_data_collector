SELECT COUNT(DISTINCT administrador) as total_administradores_unicos
FROM registro_fundo 
WHERE administrador IS NOT NULL