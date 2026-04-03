WITH OPS_BASE AS (SELECT OPS011_ID,
                         OPS011_RECURSO,
                         TO_CHAR(OPS011_DATASIMUL, 'DD/MM/YYYY') AS DATA_SIMULACAO,
                         OPS012_ORDEM,
                         OPS012_PRODUTO,
                         OPS012_PRODUTOCONF
                         
                    FROM OPTBS011
                    
                    JOIN OPTBS012 ON OPS012_OPS011_ID = OPS011_ID
                    
                   WHERE OPS011_STATUS = '2'),

TRANSFERENCIA_SITUACAO AS (SELECT RMC001_ORDEM,
                              MAX(CASE WHEN RMC001_QTDETRANSF > 0 THEN 'S' ELSE 'N' END) AS SITUACAO
                             FROM RMTBC001
                         GROUP BY RMC001_ORDEM),
                         
DADOS_COMBINADOS AS (SELECT DISTINCT OPS011_RECURSO AS RECURSO,
                                     OPS012_PRODUTO || '-' || OPS012_PRODUTOCONF AS PRODUTO,
                                     TRANSFERENCIA_SITUACAO.SITUACAO,
                                     OPS012_ORDEM AS ORDEM,
                                     OPS011_DATASIMUL AS DATA
                                     
FROM OPTBS011

JOIN OPTBS012 ON OPS012_OPS011_ID = OPS011_ID
JOIN RMTBC001 ON RMC001_ORDEM = OPS012_ORDEM
JOIN TRANSFERENCIA_SITUACAO ON TRANSFERENCIA_SITUACAO.RMC001_ORDEM = OPS012_ORDEM

WHERE OPS011_STATUS = '2'
  AND (:TRANSFERIDO = '%' OR TRANSFERENCIA_SITUACAO.SITUACAO = :TRANSFERIDO)
  AND OPS011_DATASIMUL BETWEEN TO_DATE(:DATA_INICIO, 'DD/MM/YYYY') AND TO_DATE(:DATA_FIM, 'DD/MM/YYYY')),
  
AGRUPADO AS (SELECT RECURSO,
                    PRODUTO,
                    SITUACAO,
                    'OP-' || ORDEM || ' ' || TO_CHAR(DATA, 'DD/MM/YYYY') AS OP_DATAS,
                    DATA
                    
               FROM DADOS_COMBINADOS)
               
SELECT RECURSO,
       PRODUTO,
       SITUACAO,
       LISTAGG(OP_DATAS, ' | ') WITHIN GROUP (ORDER BY DATA) AS OP_DATAS
       
FROM AGRUPADO

GROUP BY RECURSO,
         PRODUTO,
         SITUACAO
         
ORDER BY RECURSO,
         PRODUTO,
         SITUACAO