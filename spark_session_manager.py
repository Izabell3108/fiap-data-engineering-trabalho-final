# Databricks notebook source
class SparkSessionManager:
    def __init__(self, config):
        self.config = config

    def get_spark(self):
        return spark

    def stop(self):
        pass