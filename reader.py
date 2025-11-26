# Databricks notebook source
# DBTITLE 1,Reader
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType
from pyspark.sql.functions import from_json, col

class DataReader:
    def __init__(self, spark, config):
        self.spark = spark
        self.config = config

    def read_pedidos(self):
        pedidos_files = ["/Volumes/workspace/bronze/pedidos/"]
        
        return self.spark.read.csv(pedidos_files, header=True, sep=";")

    def read_pagamentos(self):
        schema = StructType([
            StructField(self.config.col_payment_order_id, StringType(), False),
            StructField(self.config.col_payment_status, BooleanType(), True),
            StructField(
                self.config.avaliacao_fraude,
                StructType([
                    StructField("fraude", BooleanType(), True),
                    StructField("score", DoubleType(), True)
                ]),
                True
            ),
            StructField(self.config.col_payment_method, StringType(), True),
        ])

        pagamentos_files = ["/Volumes/workspace/bronze/pagamentos/"]
        df = self.spark.read.json(pagamentos_files, schema=schema)

        df = df.withColumn(
            self.config.avaliacao_fraude,
            col(f"{self.config.avaliacao_fraude}.fraude")
        )

        return df

