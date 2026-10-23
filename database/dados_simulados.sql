
INSERT INTO contas (nome_cliente, numero_cartao, saldo)
VALUES
('Joao Silva', 'CARTAO001', 5000.00),
('Maria Oliveira', 'CARTAO002', 3500.00),
('Pedro Santos', 'CARTAO003', 8000.00);


INSERT INTO transacoes
    (id_conta, numero_cartao, valor, tipo_transacao)
SELECT id_conta, numero_cartao, 150.00, 'COMPRA'
FROM contas
WHERE numero_cartao = 'CARTAO001';

INSERT INTO transacoes
    (id_conta, numero_cartao, valor, tipo_transacao)
SELECT id_conta, numero_cartao, 300.00, 'COMPRA'
FROM contas
WHERE numero_cartao = 'CARTAO001';

INSERT INTO transacoes
    (id_conta, numero_cartao, valor, tipo_transacao)
SELECT id_conta, numero_cartao, 500.00, 'COMPRA'
FROM contas
WHERE numero_cartao = 'CARTAO001';
