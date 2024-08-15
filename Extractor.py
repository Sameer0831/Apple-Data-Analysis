# Databricks

# MAGIC %run "./reader_factory"

# COMMAND ----------

class Extractor:
    """
    Abstract Class
    """

    def __init__(self):
        pass

    def extract(self):
        pass

class AirpodsAfterIphoneExtractor(Extractor):
    def extract(self):
        """
        Implement the steps for extracting or reading the data
        """
        transactionInputDF = get_data_source(
            data_type="csv",
            file_path="dbfs:/FileStore/tables/transactions.csv"
        ).create_dataframe()

        transactionInputDF.orderBy("customer_id", "transaction_date").show()

        customerInputDF = get_data_source(
            data_type="delta",
            file_path="default.customer_delta_table"
        ).create_dataframe()

        customerInputDF.show()

        productsInputDF = get_data_source(
            data_type="csv",
            file_path="dbfs:/FileStore/tables/products.csv"
        ).create_dataframe()

        productsInputDF.show()

        inputDFs = {
            "transactionInputDF": transactionInputDF,
            "customerInputDF": customerInputDF,
            "productsInputDF": productsInputDF
        }

        return inputDFs
