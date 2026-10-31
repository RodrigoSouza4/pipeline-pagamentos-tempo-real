-- Cria uma view para interpretar os eventos CDC do Debezium
-- Converte os campos JSON recebidos pelo Kafka
-- Identifica operacoes de atualizacao e exclusao

CREATE VIEW eventos_contas_cdc AS
SELECT
    CAST(
        JSON_VALUE(CAST(`key` AS STRING), '$.id_conta')
        AS INT
    ) AS id_conta,

    JSON_VALUE(
        CAST(val AS STRING),
        '$.nome_cliente'
    ) AS nome_cliente,

    JSON_VALUE(
        CAST(val AS STRING),
        '$.numero_cartao'
    ) AS numero_cartao,

    CASE
        WHEN JSON_VALUE(
            CAST(val AS STRING),
            '$.__deleted'
        ) = 'true'
        THEN 'DELETE'
        ELSE 'UPDATE_AFTER'
    END AS operacao

FROM `cdc.public.contas`;