--SELECT *
--    FROM CADASTRO_PESSOAS
--      WHERE NOME = 'BRYAN' (Aqui usamos uma condição do campo)

--(A condição do campo WHERE, defini filtros também do quão especifico pode ser sua busca)
--(Existem inúmeros filtros e condições que possam ser usadas com a cláusula WHERE)

SELECT NOME, IDADE, ALTURA
    FROM CADASTRO_PESSOAS
        WHERE ALTURA = 183

--(NESTA CONSULTA O MESMO DEVE RETORNAR SOMENTE AS PESSOAS QUE POSSUEM ALTURA IGUAL A 183)