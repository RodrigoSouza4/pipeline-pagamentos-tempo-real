
# Pipeline de Pagamentos em Tempo Real

Projeto de Engenharia de Dados para processamento de transações financeiras em tempo real e identificação de possíveis fraudes.

## Objetivo

Construir um pipeline capaz de capturar transações registradas no PostgreSQL, transmiti-las ao Apache Kafka e analisá-las com Apache Flink SQL.

O sistema identifica padrões suspeitos de compras e gera alertas em um tópico Kafka.

## Tecnologias utilizadas

- **PostgreSQL:** armazenamento das contas e transações.
- **Debezium:** captura de alterações no banco de dados (CDC).
- **Apache Kafka:** transmissão dos eventos em tempo real.
- **Confluent Cloud:** infraestrutura gerenciada de Kafka e Flink.
- **Apache Flink SQL:** processamento e detecção de possíveis fraudes.
- **Docker Compose:** execução do conector Debezium.
- **Git e GitHub:** versionamento e documentação do projeto.

## Arquitetura

```text
PostgreSQL
    |
    | Change Data Capture (CDC)
    v
Debezium
    |
    v
Apache Kafka
    |
    | cdc.public.transacoes
    v
Apache Flink SQL
    |
    | Analise de transacoes
    v
Kafka: pagamentos.alertas_fraude
```

## Regra de detecção de fraude

O pipeline considera uma possível fraude quando são identificadas:

**3 ou mais compras realizadas com o mesmo cartão em uma janela de 60 segundos.**

A análise utiliza janelas deslizantes de 60 segundos, com avanço de 10 segundos, e o horário dos eventos no Kafka.

Como as janelas se sobrepõem, uma sequência de compras pode gerar mais de um alerta.

## Exemplo de alerta

```json
{
  "numero_cartao": "CARTAO001",
  "quantidade_compras": 3,
  "inicio_janela": "2026-10-23 00:06:10.000",
  "fim_janela": "2026-10-23 00:07:10.000",
  "tipo_alerta": "POSSIVEL_FRAUDE"
}
```

Os cartões apresentados são identificadores fictícios utilizados nos testes.

## Estrutura do projeto

```text
pipeline-pagamentos-tempo-real/
|
|-- database/
|   |-- criar_tabelas.sql
|   |-- configurar_cdc.sql
|   |-- configurar_usuario_cdc.sql
|   |-- dados_simulados.sql
|
|-- debezium/
|   |-- docker-compose.yml
|   |-- postgres-connector.json
|   |-- postgres-connector-backup.json
|
|-- flink/
|   |-- detectar_fraudes.sql
|
|-- .gitignore
|-- README.md
```

## Funcionamento

1. O PostgreSQL recebe as transações financeiras simuladas.
2. O Debezium captura as alterações nas tabelas.
3. O Kafka recebe os eventos nos tópicos CDC.
4. O Flink SQL analisa as compras em janelas de tempo.
5. Quando a regra é atendida, um alerta é publicado no tópico `pagamentos.alertas_fraude`.

## Segurança

As credenciais de acesso ao PostgreSQL e ao Kafka são configuradas por variáveis de ambiente.

O arquivo `.env` não é versionado no Git.

## Resultados

Durante os testes, o pipeline conseguiu:

- Capturar alterações do PostgreSQL utilizando CDC.
- Transmitir transações para o Kafka.
- Detectar sequências suspeitas de compras com Flink SQL.
- Publicar alertas no tópico Kafka de saída.

## Limitações

Este projeto é um protótipo educacional. A regra identifica padrões suspeitos, mas não confirma fraudes reais.

O uso de janelas sobrepostas pode produzir alertas repetidos. O projeto também depende de infraestrutura previamente configurada no PostgreSQL e no Confluent Cloud.


## Evidências de funcionamento

As capturas de tela dos testes estão disponíveis na pasta [evidencias](./evidencias).

### PostgreSQL
![Transações armazenadas no PostgreSQL](./evidencias/01-postgresql-transacoes.png)

### Debezium
![Contêiner Debezium](./evidencias/02-debezium-docker.png)

![Conector CDC em execução](./evidencias/03-debezium-conector-running.png)

### Apache Kafka — Transações
![Mensagens recebidas pelo Kafka](./evidencias/04-kafka-transacoes.png)

![Detalhes de uma transação](./evidencias/05-kafka-transacao-detalhada.png)

### Apache Kafka — Alertas de fraude
![Alertas publicados no Kafka](./evidencias/06-kafka-alertas-fraude.png)

![Detalhes do alerta de fraude](./evidencias/07-alerta-fraude-detalhado.png)

### Apache Flink SQL
![Processamento com Flink SQL](./evidencias/08-flink-sql-processamento.png)

![Código da detecção de fraudes](./evidencias/09-flink-sql-codigo-completo.png)

## Autor

Desenvolvido por [RodrigoSouza4](https://github.com/RodrigoSouza4).
