# Databricks notebook source

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql.functions import col, posexplode, lit, udf

# COMMAND ----------

@dp.table(
    name="grocery_documents",
    comment="Page-level text extracted from grocery industry PDF reports for Genie Q&A",
)
@dp.expect("has_text", "page_text IS NOT NULL AND length(page_text) > 10")
def grocery_documents():
    df = dp.read("grocery_pdfs_bronze")

    @udf("array<string>")
    def extract_pages(pdf_bytes):
        """Extract text from each page of a PDF."""
        import io
        try:
            from pypdf import PdfReader
            reader = PdfReader(io.BytesIO(pdf_bytes))
            return [page.extract_text() or "" for page in reader.pages]
        except Exception:
            return []

    with_pages = df.withColumn("pages", extract_pages(col("content")))

    return (
        with_pages.select(
            col("filename"),
            col("path").alias("source_path"),
            col("length").alias("file_size_bytes"),
            posexplode(col("pages")).alias("page_number", "page_text"),
        )
        .withColumn("page_number", col("page_number") + lit(1))
        .filter(col("page_text").isNotNull())
        .filter(col("page_text") != "")
    )
