# Databricks

# MAGIC %run "./loader_factory"

# COMMAND ----------

class AbstractLoader:
    def __init__(self, transformedDF):
        self.transformedDF = transformedDF

    def sink(self):
        pass

class AirpodsAfterIphoneLoader(AbstractLoader):
    def sink(self):
        get_sink_source(
           sink_type = "dbfs",
           df = self.transformedDF,
           path =  "dbfs:/FileStore/tables/output/appleAnalysis",
           method = "overwrite", 
           params=None
        ).load_data_frame()

class OnlyAirpodsAndIphoneLoader(AbstractLoader):
    def sink(self):
        params = {
            "partitionByColumns": ["location"]
        }
        get_sink_source(
           sink_type = "dbfs_with_partition",
           df = self.transformedDF,
           path =  "dbfs:/FileStore/tables/output/appleAnalysisOnlyIphones",
           method = "overwrite", 
           params = params
        ).load_data_frame()

        get_sink_source(
           sink_type = "delta",
           df = self.transformedDF,
           path =  "default.onlyAirpodsAndIphone",
           method = "overwrite", 
           params=None
        ).load_data_frame()

class ProductsAfterFirstPurchaseLoader(AbstractLoader):
    def sink(self):
        get_sink_source(
           sink_type = "dbfs",
           df = self.transformedDF,
           path =  "dbfs:/FileStore/tables/output/productsAfterFirstPurchase",
           method = "overwrite", 
           params=None
        ).load_data_frame()

class AverageTimeDelayLoader(AbstractLoader):
    def sink(self):
        get_sink_source(
           sink_type = "delta",
           df = self.transformedDF,
           path =  "default.averageTimeDelay",
           method = "overwrite", 
           params=None
        ).load_data_frame()

class TopSellingProductsLoader(AbstractLoader):
    def sink(self):
        get_sink_source(
           sink_type = "dbfs",
           df = self.transformedDF,
           path =  "dbfs:/FileStore/tables/output/topSellingProducts",
           method = "overwrite", 
           params=None
        ).load_data_frame()
