PROJETO: Sales Report – Data Engineering Programming (FIAP)

Alunos:\
  RM:361235 - Izabel Aparecida Mota Soares\
  RM: 361532 - Caio Victor Aloe da Silva Matos\
  RM: 361466 - Ricardo Antônio Ferreira Junior\
  RM: 364071 - Celso Leandro Gobetti Ferreira\
  RM: 362951 - Matheus Valentim Cornélio

Professor: Marcelo Barbosa Pinto

1. VISÃO GERAL DO PROJETO

Este projeto foi desenvolvido como trabalho final da disciplina
Data Engineering Programming, utilizando PySpark, orientação
a objetos, injeção de dependências, schemas explícitos e
pipeline de dados.

O objetivo é gerar um relatório de pedidos cuja condição seja:
- Pagamentos recusados (status = false)
- Classificados como legítimos na avaliação de fraude
  (avaliacao_fraude = false)
- Apenas pedidos do ano de 2025
- Ordenado por UF, forma de pagamento e data do pedido
- Gravado em formato Parquet

O projeto foi totalmente ajustado para funcionar na plataforma
Databricks Community Edition, utilizando Spark Connect,
schemas explícitos e tratamento de dados sujos (datas inválidas).

2. ESTRUTURA DO PROJETO

config:
    app_config → Configurações centralizadas

spark:
    spark_session_manager → Gerenciamento de sessão Spark

data_io:
    reader → Leitura de datasets
    writer → Gravação em Parquet

business:
    sales_report_service. → Lógica de negócio (filtros, joins, datas, validações)

pipeline:
    pipeline_orchestrator → Executa o pipeline completo

main → Raiz do fluxo (Aggregation Root)

tests:
    test_runner → Teste unitário adaptado ao Databricks CE

3. COMO O PROJETO FUNCIONA

  a. A classe AppConfig concentra todas as configurações:
     - nomes de colunas
     - caminhos de leitura
     - filtragem por ano
     - path de destino

  b. A classe SparkSessionManager retorna a sessão ativa gerenciada automaticamente pelo Databricks.

  c. O DataReader:
     - lê arquivos CSV (pedidos) com schema explícito
     - lê arquivos JSON (pagamentos) com struct explícito
     - extrai apenas o campo "fraude" da estrutura
       {"fraude": false, "score": 0.34}

  d. SalesReportService aplica:
     - conversão de datas com try_to_timestamp()
     - tratamento de datas inválidas
     - join entre pedidos e pagamentos
     - filtros exigidos pelo escopo
     - ordenação final

  e. PipelineOrchestrator:
     - executa a lógica de negócio
     - salva resultado em Parquet

  f. main:
     - configura dependências
     - inicializa todos os serviços
     - executa o pipeline
  
  g. Testes:
     - Adaptados para o Databricks Community Edition
     - Não usam pytest local (restrição da plataforma)
     - Executados com função custom test_runner()
     
4. COMO EXECUTAR NO DATABRICKS CE

  a. Baixe ou clone o repositório do GitHub e importe os arquivos para o Databricks CE dentro de:
     /Workspace/Users/<seu_email>/fiap-data-engineering-trabalho-final/
  
  b. Em cada notebook do Databricks, carregue os módulos necessários usando:
     %run "/Workspace/Users/<email>/fiap-data-engineering-trabalho-final/app_config"
  
  c. Para executar o pipeline, abra o notebook main e execute:
     from main import main
     main()
   
  d. Os testes unitários podem ser executados através do notebook test_runner usando:
     run_test()
    O Databricks CE não permite pytest.
      Logo, usamos: test_runner
      
5. COMO GERAR O PARQUET FINAL

O arquivo parquet é gravado automaticamente pelo pipeline:

writer.write_parquet(df)
