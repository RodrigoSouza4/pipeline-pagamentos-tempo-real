
CREATE PUBLICATION pagamentos_publication
FOR TABLE contas, transacoes
WITH (publish = 'insert, update, delete');
