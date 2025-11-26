# Databricks notebook source
# DBTITLE 1,app_config
import requests


class AppConfig:
    def __post_init__(self):
        self.url = (
            "https://github.com/infobarbosa/datasets-csv-pedidos/blob/"
            "69b13969bae364b965337160a63058eda0200326/data/pedidos/"
            "pedidos-*.csv.gz?raw=true"
        )
        local_tmp_path = "/Volumes/workspace/bronze/pedidos"
        with open(local_tmp_path, "wb") as f:
            f.write(requests.get(self.url).content)
        self.pedidos_files = [local_tmp_path]

        self.url_pagamentos = [
            "https://github.com/infobarbosa/dataset-json-pagamentos/blob/70ae72ec11341f3e9b7eca7d3232080e7d7fadea/data/pagamentos/pagamentos-*.json.gz?raw=true"
        ]
        pagamentos_files = "/Volumes/workspace/bronze/pagamentos"

        import requests
        with open(pagamentos_files, "wb") as f:
            f.write(requests.get(self.url_pagamentos[0]).content)

        self.pagamentos_files = pagamentos_files

    col_order_id = "ID_PEDIDO"
    col_order_state = "UF"
    col_order_total = "VALOR_UNITARIO"
    col_order_date = "DATA_CRIACAO"

    col_payment_order_id = "id_pedido"
    col_payment_status = "status"
    avaliacao_fraude = "avaliacao_fraude"
    col_payment_method = "forma_pagamento"

    filter_year = 2025