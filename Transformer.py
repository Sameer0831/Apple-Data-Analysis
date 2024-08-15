from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("apple data analysis").getOrCreate()

from pyspark.sql.window import Window
from pyspark.sql.functions import lead, col, broadcast, collect_set, size, array_contains, sum

class Transformer:
    def __init__(self):
        pass

    def transform(self, inputDFs):
        pass

class AirpodsAfterIphoneTransformer(Transformer):
    def transform(self, inputDFs):
        transactionInputDF = inputDFs.get("transactionInputDF")
        windowSpec = Window.partitionBy("customer_id").orderBy("transaction_date")

        transformedDF = transactionInputDF.withColumn(
            "next_purchased_product", lead("product_name").over(windowSpec)
        )

        filteredDF = transformedDF.filter(
            ((col("product_name") == "iPhone") & (col("next_purchased_product") == "AirPods"))
        )

        customerInputDF = inputDFs.get("customerInputDF")

        joinDF = customerInputDF.join(
            broadcast(filteredDF), 
            "customer_id"
        )

        return joinDF.select(
            "customer_id",
            "customer_name",
            "location"
        )

class OnlyAirpodsAndIphoneTransformer(Transformer):
    def transform(self, inputDFs):
        transactionInputDF = inputDFs.get("transactionInputDF")
        groupedDF = transactionInputDF.groupBy("customer_id").agg(
            collect_set("product_name").alias("products")
        )

        filteredDF = groupedDF.filter(
            ((array_contains(col("products"), "iPhone")) & 
              (array_contains(col("products"), "AirPods")) &
              (size(col("products")) == 2)
            )
        )

        customerInputDF = inputDFs.get("customerInputDF")

        joinDF = customerInputDF.join(
            broadcast(filteredDF), 
            "customer_id"
        )

        return joinDF.select(
            "customer_id",
            "customer_name",
            "location"
        )

class ListProductsAfterFirstPurchaseTransformer(Transformer):
    def transform(self, inputDFs):
        transactionInputDF = inputDFs.get("transactionInputDF")

        windowSpec = Window.partitionBy("customer_id").orderBy("transaction_date")

        productsAfterFirstPurchaseDF = transactionInputDF.withColumn(
            "row_num", row_number().over(windowSpec)
        ).filter(col("row_num") > 1)

        return productsAfterFirstPurchaseDF.select("customer_id", "product_name")

class AverageTimeDelayTransformer(Transformer):
    def transform(self, inputDFs):
        transactionInputDF = inputDFs.get("transactionInputDF")

        windowSpec = Window.partitionBy("customer_id").orderBy("transaction_date")

        delayDF = transactionInputDF.withColumn(
            "next_purchased_product", lead("product_name").over(windowSpec)
        ).withColumn(
            "time_delay", datediff(lead("transaction_date").over(windowSpec), col("transaction_date"))
        ).filter(
            ((col("product_name") == "iPhone") & (col("next_purchased_product") == "AirPods"))
        )

        averageDelayDF = delayDF.groupBy("customer_id").agg(
            avg("time_delay").alias("average_time_delay")
        )

        return averageDelayDF.select("customer_id", "average_time_delay")

class TopSellingProductsTransformer(Transformer):
    def transform(self, inputDFs):
        transactionInputDF = inputDFs.get("transactionInputDF")
        productsInputDF = inputDFs.get("productsInputDF")

        joinedDF = transactionInputDF.join(
            productsInputDF, 
            "product_id"
        )

        topProductsDF = joinedDF.groupBy("category", "product_name").agg(
            sum("revenue").alias("total_revenue")
        ).orderBy("category", col("total_revenue").desc())

        windowSpec = Window.partitionBy("category").orderBy(col("total_revenue").desc())

        rankedDF = topProductsDF.withColumn(
            "rank", row_number().over(windowSpec)
        ).filter(col("rank") <= 3)

        return rankedDF.select("category", "product_name", "total_revenue")
