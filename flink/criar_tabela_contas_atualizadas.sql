
-- Pipeline de Pagamentos em Tempo Real
-- Tabela para manter o estado atualizado dos clientes

CREATE TABLE contas_atualizadas (
    id_conta INT,
    nome_cliente STRING,
    numero_cartao STRING,
    PRIMARY KEY (id_conta) NOT ENFORCED
)
DISTRIBUTED INTO 1 BUCKETS
WITH (
    'changelog.mode' = 'upsert'
);
