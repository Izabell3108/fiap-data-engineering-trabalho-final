# Databricks notebook source
# DBTITLE 1,Writer
class DataWriter:
    def __init__(self, spark, config):
        self.spark = spark
        self.config = config

    def write_parquet(self, df, path=None, mode="overwrite"):
        out = "/Volumes/workspace/silver/sales_reporte_service"
        df.write.mode(mode).parquet(out)
