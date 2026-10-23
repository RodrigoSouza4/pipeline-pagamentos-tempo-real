
-- Pipeline de Pagamentos em Tempo Real
-- Deteccao de possiveis fraudes com Flink SQL
-- Regra: 3 ou mais compras do mesmo cartao em 60 segundos

INSERT INTO `pagamentos.alertas_fraude` (`key`, `val`)

WITH transacoes AS (
    SELECT
        JSON_VALUE(
            CAST(val AS STRING),
            '$.numero_cartao'
        ) AS numero_cartao,

        JSON_VALUE(
            CAST(val AS STRING),
            '$.tipo_transacao'
        ) AS tipo_transacao,

        $rowtime AS horario_kafka

    FROM `cdc.public.transacoes`

    WHERE JSON_VALUE(
        CAST(val AS STRING),
        '$.id_transacao'
    ) IS NOT NULL
),

alertas AS (
    SELECT
        numero_cartao,
        window_start,
        window_end,
        COUNT(*) AS quantidade_compras

    FROM TABLE(
        HOP(
            TABLE transacoes,
            DESCRIPTOR(horario_kafka),
            INTERVAL '10' SECOND,
            INTERVAL '60' SECOND
        )
    )

    WHERE tipo_transacao = 'COMPRA'

    GROUP BY
        numero_cartao,
        window_start,
        window_end

    HAVING COUNT(*) >= 3
)

SELECT
    CAST(numero_cartao AS BYTES),

    CAST(
        JSON_OBJECT(
            'numero_cartao' VALUE numero_cartao,
            'quantidade_compras' VALUE quantidade_compras,
            'inicio_janela' VALUE CAST(window_start AS STRING),
            'fim_janela' VALUE CAST(window_end AS STRING),
            'tipo_alerta' VALUE 'POSSIVEL_FRAUDE'
        ) AS BYTES
    )

FROM alertas;
