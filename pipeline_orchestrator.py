# Databricks notebook source
from pyspark.sql.functions import expr


class PipelineOrchestrator:
    def __init__(self, service, writer):
        self.service = service
        self.writer = writer

    def run(self):
        df = self.service.build_report()
        df.display(20)
        self.writer.write_parquet(df)
