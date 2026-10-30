
-- =====================================================
-- Configuração do usuário CDC para o Debezium
-- Banco de dados: pagamentos_db
-- =====================================================

-- Execute este script como administrador do PostgreSQL.
-- Substitua a senha de exemplo por uma senha segura
-- antes de executar em uma nova instalação.
-- Não publique credenciais reais no GitHub.

-- 1. Criar usuário com permissão de replicação
-- Execute apenas se o usuário ainda não existir.
CREATE ROLE cdc_user
WITH LOGIN REPLICATION
PASSWORD 'SUBSTITUIR_POR_SENHA_SEGURA';

-- 2. Permitir conexão ao banco de dados
GRANT CONNECT ON DATABASE pagamentos_db TO cdc_user;

-- 3. Permitir acesso ao esquema público
GRANT USAGE ON SCHEMA public TO cdc_user;

-- 4. Permitir leitura das tabelas utilizadas pelo CDC
GRANT SELECT ON TABLE public.contas TO cdc_user;
GRANT SELECT ON TABLE public.transacoes TO cdc_user;
