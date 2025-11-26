# Databricks notebook source
# MAGIC %md
# MAGIC **Lembre-se:** Alterar o e-mail para conseguir realizar a execução

# COMMAND ----------

# DBTITLE 1,Chamada app_config
# MAGIC %run /Workspace/Users/comercial.wbs@icloud.com/sales-report/app_config

# COMMAND ----------

# DBTITLE 1,Chamada Spark
# MAGIC %run /Workspace/Users/comercial.wbs@icloud.com/sales-report/spark_session_manager

# COMMAND ----------

# DBTITLE 1,Chamada Reader
# MAGIC %run /Workspace/Users/comercial.wbs@icloud.com/sales-report/reader

# COMMAND ----------

# DBTITLE 1,Chamada Writer
# MAGIC %run /Workspace/Users/comercial.wbs@icloud.com/sales-report/writer

# COMMAND ----------

# DBTITLE 1,Chamada  Sales_report
# MAGIC %run /Workspace/Users/comercial.wbs@icloud.com/sales-report/sales_report_service

# COMMAND ----------

# DBTITLE 1,Chamada Orchestrator
# MAGIC %run /Workspace/Users/comercial.wbs@icloud.com/sales-report/pipeline_orchestrator

# COMMAND ----------

# DBTITLE 1,main
config = AppConfig()
spark_mgr = SparkSessionManager(config)
spark_session = spark_mgr.get_spark()

reader = DataReader(spark_session, config)
writer = DataWriter(spark_session, config)
service = SalesReportService(spark_session, config, reader, writer)
pipeline = PipelineOrchestrator(service, writer)

pipeline.run()

print("Pipeline executado!")
