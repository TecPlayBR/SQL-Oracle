WITH RankedData AS (SELECT DISTINCT DECODE(NEB001_PRODUTOCONF, 0, NEB001_PRODUTO, NEB001_PRODUTO || '-' || NEB001_PRODUTOCONF) PRODUTO,
                                           NEB001_PRODDESC DESCRICAO,
                                           NEB001_QUANTIDADE QTD,
                                           NEB001_TIPODOC || '-' || NEB001_NOTA || '-' || NEB001_SERIE NOTA,
                                           IEA002_TIPODOC || '-' || IEA002_PROCESSO PROCESSO,
                                           IEA001_DTPREVISAO PREVISAO_PIMP,
                                           ROW_NUMBER() 
                                           OVER (PARTITION BY NEB001_PRODUTO ORDER BY IEA001_DTPREVISAO DESC) AS RN
                                           
                    FROM NETBB001
                            
                    JOIN OCTBB001 ON OCB001_GRUPO = NEB001_GRUPO
                                 AND OCB001_EMPRESA = NEB001_EMPRESA
                                 AND OCB001_FILIAL = NEB001_FILIAL
                                 AND OCB001_TIPODOC = NEB001_TIPODOCOC
                                 AND OCB001_ORDCOMPRA = NEB001_ORDCOMPRA
                                 AND OCB001_LINHA = NEB001_LINHAOC
                                 
                    JOIN IETBA002 ON NEB001_GRUPO = IEA002_GRUPO
                                 AND NEB001_EMPRESA = IEA002_EMPRESA
                                 AND NEB001_FILIAL = IEA002_FILIAL
                                 AND NEB001_TIPODOCOC = IEA002_TIPODOCOC
                                 AND NEB001_ORDCOMPRA = IEA002_ORDCOMPRA
                                 
                    JOIN IETBA001 ON IEA002_GRUPO = IEA001_GRUPO
                                 AND IEA002_EMPRESA = IEA001_EMPRESA
                                 AND IEA002_FILIAL = IEA001_FILIAL
                                 AND IEA002_TIPODOC = IEA001_TIPODOC
                                 AND IEA002_PROCESSO = IEA001_PROCESSO
                                 
                    LEFT JOIN ESTBC002 ON ESC002_GRUPO = NEB001_GRUPO
                                      AND ESC002_EMPRESA = NEB001_EMPRESA
                                      AND ESC002_FILIAL = NEB001_FILIAL
                                      AND ESC002_PRODUTO = NEB001_PRODUTO
                                      AND ESC002_PRODUTOCONF = NEB001_PRODUTOCONF
                                      AND ESC002_ALMOXARIFADO = NEB001_ALMOXARIFADO
                                      AND ESC002_QUANTIDADE > 0
                                
                    LEFT JOIN ESTBB001 ON ESB001_GRUPO = NEB001_GRUPO
                                      AND ESB001_EMPRESA = NEB001_EMPRESA
                                      AND ESB001_FILIAL = NEB001_FILIAL
                                      AND ESB001_TIPODOC = NEB001_TIPODOC
                                      AND ESB001_NRODOC = NEB001_NOTA
                                      AND ESB001_SER = NEB001_SERIE
                                      AND ESB001_LINHADOC = NEB001_LINHA
                                      AND ESB001_PRODUTO = NEB001_PRODUTO
                                      AND ESB001_PRODUTOCONF = NEB001_PRODUTOCONF
                                      AND ESB001_ALMOXARIFADO = 88
                                      AND ESB001_TIPOMOV = 'S'
                                      
                    WHERE NEB001_ALMOXARIFADO = 88
                      AND NVL(NEB001_LCTOESTPRINCIPAL, 0) <> 0
                      AND NEB001_PRODUTO LIKE NVL(:PRODUTO, '%')
                      AND IEA001_DTPREVISAO IS NOT NULL) /*VERIFICAR SE MANTÉM DADOS SOMENTE COM DADO OU NÃO, SE NÃO, ENTÃO LISTA TODOS COM BASE NO PROCESO -> PIMP*/
  
SELECT PRODUTO,
       DESCRICAO,
       QTD,
       NOTA,
       PROCESSO,
       PREVISAO_PIMP
       
FROM RankedData

WHERE RN = 1