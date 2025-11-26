# Databricks notebook source
# DBTITLE 1,Sales Report Service
import logging
from pyspark.sql.functions import expr, year, col

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class SalesReportService:
    def __init__(self, spark, config, reader, writer):
        self.spark = spark
        self.config = config
        self.reader = reader
        self.writer = writer
        self.logger = logging.getLogger(self.__class__.__name__)

    def build_report(self):
        try:
            self.logger.info("Lendo pedidos")
            pedidos = self.reader.read_pedidos()

            self.logger.info("Lendo pagamentos")
            pagamentos = self.reader.read_pagamentos()

            self.logger.info("Convertendo coluna de data (data_pedido)")
            pedidos = pedidos.withColumn(
                "order_ts",
                expr("try_to_timestamp({})".format(self.config.col_order_date))
            )

            invalid_count = pedidos.filter("order_ts IS NULL").count()
            if invalid_count > 0:
                self.logger.warning(
                    f"{invalid_count} linhas possuem data_pedido inválido e foram descartadas."
                )


            pedidos = pedidos.filter("order_ts IS NOT NULL")

            self.logger.info("Unindo pedidos com pagamentos")
            joined = pedidos.join(
                pagamentos,
                pedidos[self.config.col_order_id] == pagamentos[self.config.col_payment_order_id]
            )

            self.logger.info("Filtrando pagamentos recusados e não fraudulentos no ano de 2025")
            filtered = joined.filter(
                (col(self.config.col_payment_status) == False) &
                (col(self.config.avaliacao_fraude) == False) &
                (year(col("order_ts")) == self.config.filter_year)
            )

            self.logger.info("Selecionando colunas finais")
            result = filtered.select(
                pedidos[self.config.col_order_id].alias("id_pedido"),
                pedidos[self.config.col_order_state].alias("uf"),
                pagamentos[self.config.col_payment_method].alias("forma_pagamento"),
                pedidos[self.config.col_order_total].alias("valor_total_pedido"),
                col("order_ts").alias("data_pedido")
            )

            self.logger.info("Ordenando resultados")
            return result.orderBy("uf", "forma_pagamento", "data_pedido")

        except Exception as e:
            self.logger.exception(f"Erro: {e}")
            raise
