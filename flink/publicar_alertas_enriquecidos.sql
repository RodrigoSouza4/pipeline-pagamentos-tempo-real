
-- Pipeline de Pagamentos em Tempo Real
-- Publicacao de alertas de fraude enriquecidos
-- Regra: 3 ou mais compras do mesmo cartao em 60 segundos

INSERT INTO `pagamentos.alertas_fraude_enriquecidos` (`key`, `val`)

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
    SELECT
        id_conta,
        nome_cliente
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
    WHERE JSON_VALUE(
        CAST(val AS STRING),
        '$.tipo_transacao'
    ) = 'COMPRA'
      AND JSON_VALUE(
        CAST(val AS STRING),
        '$.__deleted'
    ) = 'false'
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
),

alertas_enriquecidos AS (
    SELECT
        c.nome_cliente,
        a.numero_cartao,
        a.quantidade_compras,
        a.window_start,
        a.window_end,
        ROW_NUMBER() OVER (
            PARTITION BY
                a.numero_cartao,
                a.window_start,
                a.window_end
            ORDER BY c.nome_cliente
        ) AS rn
    FROM alertas AS a
    JOIN contas_ativas AS c
        ON a.id_conta = c.id_conta
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
FROM alertas_enriquecidos
WHERE rn = 1;
