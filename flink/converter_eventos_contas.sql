
-- Converte os eventos CDC do Debezium em atualizacoes
-- e exclusoes da tabela contas_atualizadas.

INSERT INTO contas_atualizadas
    (id_conta, nome_cliente, numero_cartao)

SELECT
    id_conta,
    nome_cliente,
    numero_cartao

FROM FROM_CHANGELOG(
    input => TABLE (
        SELECT
            CAST(
                JSON_VALUE(
                    CAST(`key` AS STRING),
                    '$.id_conta'
                ) AS INT
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

        FROM `cdc.public.contas`
    ) PARTITION BY id_conta,

    op => DESCRIPTOR(operacao)
);
