
-- Pipeline de Pagamentos em Tempo Real
-- Deteccao de fraudes enriquecida com dados do cliente
-- Regra: 3 ou mais compras do mesmo cartao em 60 segundos

WITH eventos_contas AS (
    SELECT
        JSON_VALUE(CAST(val AS STRING), '$.id_conta') AS id_conta,
        JSON_VALUE(CAST(val AS STRING), '$.nome_cliente') AS nome_cliente,
        JSON_VALUE(CAST(val AS STRING), '$.__deleted') AS excluida,
        $rowtime AS horario_evento
    FROM `cdc.public.contas`
),

contas_ordenadas AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY id_conta
            ORDER BY horario_evento DESC
        ) AS posicao
    FROM eventos_contas
),

contas_ativas AS (
    SELECT id_conta, nome_cliente
    FROM contas_ordenadas
    WHERE posicao = 1
      AND excluida = 'false'
),

compras AS (
    SELECT
        JSON_VALUE(CAST(val AS STRING), '$.id_conta') AS id_conta,
        JSON_VALUE(CAST(val AS STRING), '$.numero_cartao') AS numero_cartao,
        $rowtime AS horario_evento
    FROM `cdc.public.transacoes`
    WHERE JSON_VALUE(CAST(val AS STRING), '$.tipo_transacao') = 'COMPRA'
      AND JSON_VALUE(CAST(val AS STRING), '$.__deleted') = 'false'
),

alertas AS (
    SELECT
        id_conta,
        numero_cartao,
        window_start,
        window_end,
        COUNT(*) AS quantidade_compras
    FROM TABLE(
        HOP(
            TABLE compras,
            DESCRIPTOR(horario_evento),
            INTERVAL '10' SECOND,
            INTERVAL '60' SECOND
        )
    )
    GROUP BY
        id_conta,
        numero_cartao,
        window_start,
        window_end
    HAVING COUNT(*) >= 3
)

SELECT
    c.nome_cliente,
    a.numero_cartao,
    a.quantidade_compras,
    a.window_start,
    a.window_end,
    'POSSIVEL_FRAUDE' AS tipo_alerta
FROM alertas AS a
JOIN contas_ativas AS c
    ON a.id_conta = c.id_conta;
