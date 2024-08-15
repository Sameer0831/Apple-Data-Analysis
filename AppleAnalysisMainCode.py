# Databricks notebook source
# MAGIC %run "./transform"

# COMMAND ----------

# MAGIC %run "./extractor"

# COMMAND ----------

# MAGIC %run "./loader"

# COMMAND ----------

class FirstWorkFlow:
    def __init__(self):
        pass

    def runner(self):
        inputDFs = AirpodsAfterIphoneExtractor().extract()
        firstTransformedDF = AirpodsAfterIphoneTransformer().transform(inputDFs)
        AirpodsAfterIphoneLoader(firstTransformedDF).sink()

class SecondWorkFlow:
    def __init__(self):
        pass

    def runner(self):
        inputDFs = AirpodsAfterIphoneExtractor().extract()
        onlyAirpodsAndIphoneDF = OnlyAirpodsAndIphoneTransformer().transform(inputDFs)
        OnlyAirpodsAndIphoneLoader(onlyAirpodsAndIphoneDF).sink()

class ThirdWorkFlow:
    def __init__(self):
        pass

    def runner(self):
        inputDFs = AirpodsAfterIphoneExtractor().extract()
        productsAfterFirstPurchaseDF = ListProductsAfterFirstPurchaseTransformer().transform(inputDFs)
        ProductsAfterFirstPurchaseLoader(productsAfterFirstPurchaseDF).sink()

class FourthWorkFlow:
    def __init__(self):
        pass

    def runner(self):
        inputDFs = AirpodsAfterIphoneExtractor().extract()
        averageTimeDelayDF = AverageTimeDelayTransformer().transform(inputDFs)
        AverageTimeDelayLoader(averageTimeDelayDF).sink()

class FifthWorkFlow:
    def __init__(self):
        pass

    def runner(self):
        inputDFs = AirpodsAfterIphoneExtractor().extract()
        topSellingProductsDF = TopSellingProductsTransformer().transform(inputDFs)
        TopSellingProductsLoader(topSellingProductsDF).sink()

class WorkFlowRunner:
    def __init__(self, workflow_name):
        self.workflow_name = workflow_name

    def runner(self):
        if self.workflow_name.lower() == "firstworkflow":
            FirstWorkFlow().runner()
        elif self.workflow_name.lower() == "secondworkflow":
            SecondWorkFlow().runner()
        elif self.workflow_name.lower() == "thirdworkflow":
            ThirdWorkFlow().runner()
        elif self.workflow_name.lower() == "fourthworkflow":
            FourthWorkFlow().runner()
        elif self.workflow_name.lower() == "fifthworkflow":
            FifthWorkFlow().runner()
        else:
            raise ValueError(f"Workflow {self.workflow_name} not found. Please provide a valid workflow name.")

# Example usage:
workflow_name = "secondworkflow"  # Replace with your desired workflow
workFlowRunner = WorkFlowRunner(workflow_name).runner()
