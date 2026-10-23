
CREATE TABLE contas (
    id_conta SERIAL PRIMARY KEY,
    nome_cliente VARCHAR(100) NOT NULL,
    numero_cartao VARCHAR(20) NOT NULL UNIQUE,
    saldo DECIMAL(12,2) NOT NULL DEFAULT 0,
    data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE transacoes (
    id_transacao SERIAL PRIMARY KEY,
    id_conta INTEGER NOT NULL,
    numero_cartao VARCHAR(20) NOT NULL,
    valor DECIMAL(12,2) NOT NULL,
    tipo_transacao VARCHAR(20) NOT NULL,
    data_transacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(20) DEFAULT 'APROVADA',

    CONSTRAINT fk_conta
        FOREIGN KEY (id_conta)
        REFERENCES contas(id_conta)
);
