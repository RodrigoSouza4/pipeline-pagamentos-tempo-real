-- Conversao dos eventos CDC para um changelog
-- Utiliza a view eventos_contas_cdc
-- Consulta validada no Flink SQL em modo Streaming

SELECT
    id_conta,
    nome_cliente,
    numero_cartao
FROM TABLE(
    FROM_CHANGELOG(
        input => TABLE eventos_contas_cdc
            PARTITION BY id_conta,
        op => DESCRIPTOR(operacao)
    )
);