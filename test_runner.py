# Databricks notebook source
# MAGIC %md
# MAGIC **Lembre-se**: Alterar o e-mail para conseguir realizar a execução 

# COMMAND ----------

# DBTITLE 1,Chamada app_config
# MAGIC %run /Workspace/Users/comercial.wbs@icloud.com/sales-report/app_config

# COMMAND ----------

# DBTITLE 1,Chamada Writer
# MAGIC %run /Workspace/Users/comercial.wbs@icloud.com/sales-report/writer

# COMMAND ----------

# DBTITLE 1,Chamada Sales report
# MAGIC %run /Workspace/Users/comercial.wbs@icloud.com/sales-report/sales_report_service

# COMMAND ----------

def run_test():
    class TestConfig(AppConfig):
        filter_year = 2025

    cfg = TestConfig()

    data_pedidos = [
        ("p1", "SP", 100.0, "2025-02-10 10:00:00")
    ]
    pedidos_schema = "ID_PEDIDO STRING, UF STRING, VALOR_UNITARIO DOUBLE, DATA_CRIACAO STRING"
    df_pedidos = spark.createDataFrame(
        data_pedidos,
        pedidos_schema
    )

    data_pag = [("p1", False, False, "cartao")]
    pag_schema = "id_pedido STRING, status BOOLEAN, avaliacao_fraude BOOLEAN, forma_pagamento STRING"
    df_pag = spark.createDataFrame(
        data_pag,
        pag_schema
    )

    class DummyReader:
        def read_pedidos(self):
            return df_pedidos
        def read_pagamentos(self):
            return df_pag

    writer = DataWriter(spark, cfg)
    service = SalesReportService(
        spark,
        cfg,
        DummyReader(),
        writer
    )

    df = service.build_report()
    rows = df.collect()

    assert rows[0]["id_pedido"] == "p1"
    print("TESTE OK!")

run_test()
