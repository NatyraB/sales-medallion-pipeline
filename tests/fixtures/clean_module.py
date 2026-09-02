# Databricks notebook source
"""A tidy, healthy pipeline-style module used as a positive fixture."""
import logging

from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


def transform(df):
    """Select and rename a column."""
    return df.select(F.col("account_id").alias("id"))


logger.info("clean_module ready")
