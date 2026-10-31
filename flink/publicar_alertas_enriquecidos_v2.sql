
-- Pipeline de Pagamentos em Tempo Real
-- Publicacao de alertas de fraude enriquecidos - V2
-- Regra: 3 ou mais compras do mesmo cartao em 60 segundos

INSERT INTO `pagamentos.alertas_fraude_enriquecidos` (`key`, `val`)

WITH contas AS (
    SELECT
        JSON_VALUE(CAST(val AS STRING), '$.id_conta') AS id_conta,
        JSON_VALUE(CAST(val AS STRING), '$.nome_cliente') AS nome_cliente
    FROM `cdc.public.contas`
    WHERE JSON_VALUE(CAST(val AS STRING), '$.__deleted') = 'false'
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

compras_enriquecidas AS (
    SELECT
        c.nome_cliente,
        t.id_conta,
        t.numero_cartao,
        t.horario_evento
    FROM compras AS t
    JOIN contas AS c
        ON t.id_conta = c.id_conta
),

alertas AS (
    SELECT
        nome_cliente,
        numero_cartao,
        window_start,
        window_end,
        COUNT(*) AS quantidade_compras
    FROM TABLE(
        HOP(
            TABLE compras_enriquecidas,
            DESCRIPTOR(horario_evento),
            INTERVAL '10' SECOND,
            INTERVAL '60' SECOND
        )
    )
    GROUP BY
        nome_cliente,
        numero_cartao,
        window_start,
        window_end
    HAVING COUNT(*) >= 3
)

SELECT
    CAST(numero_cartao AS BYTES) AS `key`,
    CAST(
        JSON_OBJECT(
            'nome_cliente' VALUE nome_cliente,
            'numero_cartao' VALUE numero_cartao,
            'quantidade_compras' VALUE quantidade_compras,
            'inicio_janela' VALUE CAST(window_start AS STRING),
            'fim_janela' VALUE CAST(window_end AS STRING),
            'tipo_alerta' VALUE 'POSSIVEL_FRAUDE'
        ) AS BYTES
    ) AS `val`
FROM alertas;
