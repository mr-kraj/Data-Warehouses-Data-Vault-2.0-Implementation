from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as spark_sum, col, monotonically_increasing_id, coalesce, lit

def load_bronze(spark):
    jdbc_url = (
        "jdbc:sqlserver://host.docker.internal:1433;"
        "databaseName=AdventureWorksDW2022;"
        "encrypt=true;"
        "trustServerCertificate=true"
    )

    jdbc_props = {
        "user": "admin",
        "password": "admin",
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }

    # OGRANICZENIE ILOŚCI DANYCH
    """
    #dim_customer = spark.read.jdbc(jdbc_url, "DimCustomer", properties=jdbc_props)
    #dim_product = spark.read.jdbc(jdbc_url, "DimProduct", properties=jdbc_props)
    #dim_date = spark.read.jdbc(jdbc_url, "DimDate", properties=jdbc_props)

    fact_sales = spark.read.jdbc(
    url=jdbc_url,
    table="FactInternetSales",
    column="SalesOrderLineNumber",
    lowerBound=1,            
    upperBound=60000,         
    numPartitions=10,         
    properties=jdbc_props
    )

    """

    dim_customer = spark.read.jdbc(jdbc_url, "(SELECT TOP 1000 * FROM DimCustomer) AS tmp", properties=jdbc_props)
    dim_product = spark.read.jdbc(jdbc_url, "(SELECT TOP 1000 * FROM DimProduct) AS tmp", properties=jdbc_props)
    dim_date = spark.read.jdbc(jdbc_url, "(SELECT TOP 1000 * FROM DimDate) AS tmp", properties=jdbc_props)

    fact_sales = spark.read.jdbc(
        url=jdbc_url,
        table="(SELECT TOP 5000 * FROM FactInternetSales) AS tmp",
        column="SalesOrderLineNumber",
        lowerBound=1,            
        upperBound=1000,         
        numPartitions=4,         
        properties=jdbc_props
    )

    hub_customer = dim_customer.select("CustomerKey").withColumn("HubCustomerID", monotonically_increasing_id())
    hub_product = dim_product.select("ProductKey").withColumn("HubProductID", monotonically_increasing_id())
    hub_date = dim_date.select("DateKey").withColumn("HubDateID", monotonically_increasing_id())

    link_sales = fact_sales \
        .join(hub_customer, fact_sales.CustomerKey == hub_customer.CustomerKey) \
        .join(hub_product, fact_sales.ProductKey == hub_product.ProductKey) \
        .join(hub_date, fact_sales.OrderDateKey == hub_date.DateKey) \
        .select(
            "SalesOrderNumber",
            col("HubCustomerID"),
            col("HubProductID"),
            col("HubDateID")
        ) \
        .withColumn("LinkSalesID", monotonically_increasing_id())

    sat_customer = dim_customer \
        .join(hub_customer, "CustomerKey") \
        .select(
            col("HubCustomerID"),
            "FirstName",
            "LastName",
            "EmailAddress",
            "Phone"
        )

    sat_product = dim_product \
        .join(hub_product, "ProductKey") \
        .select(
            col("HubProductID"),
            col("EnglishProductName").alias("ProductName"),
            "ProductAlternateKey",
            "Color",
            "StandardCost",
            "ListPrice"
        )

    sat_date = dim_date \
        .join(hub_date, "DateKey") \
        .select(
            col("HubDateID"),
            "FullDateAlternateKey",
            "DayNumberOfWeek",
            "EnglishMonthName",
            "CalendarQuarter",
            "CalendarYear"
        )

    sat_sales = fact_sales \
        .join(link_sales, ["SalesOrderNumber"]) \
        .select(
            col("LinkSalesID"),
            "OrderQuantity",
            "UnitPrice",
            "ExtendedAmount",
            "TotalProductCost",
            "SalesAmount",
            "TaxAmt",
            "Freight"
        )

    base_path = "/data/datalake/bronze"

    for hub_df, name in [
        (hub_customer, "hub_customer"),
        (hub_product, "hub_product"),
        (hub_date, "hub_date"),
    ]:
        hub_df.write.format("hudi") \
            .option("hoodie.table.name", name) \
            .option("hoodie.datasource.write.recordkey.field", hub_df.columns[1]) \
            .option("hoodie.datasource.write.precombine.field", hub_df.columns[1]) \
            .mode("append") \
            .save(f"{base_path}/hubs/{name}")

    link_sales.write.format("hudi") \
        .option("hoodie.table.name", "link_sales") \
        .option("hoodie.datasource.write.recordkey.field", "LinkSalesID") \
        .option("hoodie.datasource.write.precombine.field", "LinkSalesID") \
        .mode("append") \
        .save(f"{base_path}/links/link_sales")

    for sat_df, name in [
        (sat_customer, "sat_customer"),
        (sat_product, "sat_product"),
        (sat_date, "sat_date"),
        (sat_sales, "sat_sales")
    ]:
        sat_df.write.format("hudi") \
            .option("hoodie.table.name", name) \
            .option("hoodie.datasource.write.recordkey.field", sat_df.columns[0]) \
            .option("hoodie.datasource.write.precombine.field", sat_df.columns[0]) \
            .mode("append") \
            .save(f"{base_path}/satellites/{name}")

def load_silver(spark):
    base_bronze = "/data/datalake/bronze"
    base_silver = "/data/datalake/silver"

    hub_customer = spark.read.format("hudi").load(f"{base_bronze}/hubs/hub_customer/*")
    hub_product = spark.read.format("hudi").load(f"{base_bronze}/hubs/hub_product/*")
    hub_date = spark.read.format("hudi").load(f"{base_bronze}/hubs/hub_date/*")

    link_sales = spark.read.format("hudi").load(f"{base_bronze}/links/link_sales/*")

    sat_customer = spark.read.format("hudi").load(f"{base_bronze}/satellites/sat_customer/*")
    sat_product = spark.read.format("hudi").load(f"{base_bronze}/satellites/sat_product/*")
    sat_date = spark.read.format("hudi").load(f"{base_bronze}/satellites/sat_date/*")
    sat_sales = spark.read.format("hudi").load(f"{base_bronze}/satellites/sat_sales/*")

    silver_sales = link_sales \
        .join(sat_customer, "HubCustomerID", "left") \
        .join(sat_product, "HubProductID", "left") \
        .join(sat_date, "HubDateID", "left") \
        .join(sat_sales, "LinkSalesID", "left")

    silver_sales_clean = silver_sales \
        .withColumn("FirstName", coalesce(col("FirstName"), lit("UNKNOWN"))) \
        .withColumn("LastName", coalesce(col("LastName"), lit("UNKNOWN"))) \
        .withColumn("SalesAmount", coalesce(col("SalesAmount"), lit(0))) \
        .filter(col("SalesAmount") >= 0) \
        .withColumnRenamed("FullDateAlternateKey", "OrderDate") \
        .withColumnRenamed("EnglishMonthName", "MonthName") \
        .withColumnRenamed("CalendarYear", "Year")

    silver_sales_clean.write.format("hudi") \
        .option("hoodie.table.name", "silver_sales") \
        .option("hoodie.datasource.write.recordkey.field", "LinkSalesID") \
        .option("hoodie.datasource.write.precombine.field", "LinkSalesID") \
        .mode("append") \
        .save(f"{base_silver}/silver_sales")

def load_gold(spark):
    base_silver = "/data/datalake/silver"
    base_gold = "/data/datalake/gold"

    silver_sales = spark.read.format("hudi").load(f"{base_silver}/silver_sales/*")

    gold_fact_sales = silver_sales.groupBy(
        "HubProductID",
        "Year",
        "MonthName"
    ).agg(
        spark_sum("SalesAmount").alias("TotalSales"),
        spark_sum("OrderQuantity").alias("TotalQuantity"),
        spark_sum("Freight").alias("TotalFreight")
    )

    gold_sales = gold_fact_sales \
        .join(
            silver_sales
                .select("HubProductID", "Name", "ProductNumber", "Color")
                .dropDuplicates(["HubProductID"]),
            "HubProductID",
            "left"
        )
    
    gold_dim_customer = silver_sales.select(
        "HubCustomerID", "FirstName", "LastName", "EmailAddress"
    ).dropDuplicates(["HubCustomerID"])

    gold_dim_product = silver_sales.select(
        "HubProductID", "Name", "ProductNumber", "Color"
    ).dropDuplicates(["HubProductID"])

    gold_dim_date = silver_sales.select(
        "HubDateID", "OrderDate", "MonthName", "Year"
    ).dropDuplicates(["HubDateID"])

    gold_sales.write.format("hudi") \
        .option("hoodie.table.name", "gold_fact_sales") \
        .option("hoodie.datasource.write.recordkey.field", "HubProductID") \
        .option("hoodie.datasource.write.precombine.field", "HubProductID") \
        .mode("append") \
        .save(f"{base_gold}/fact_sales")

    gold_dim_customer.write.format("hudi") \
        .option("hoodie.table.name", "gold_dim_customer") \
        .option("hoodie.datasource.write.recordkey.field", "HubCustomerID") \
        .option("hoodie.datasource.write.precombine.field", "HubCustomerID") \
        .mode("append") \
        .save(f"{base_gold}/dim_customer")

    gold_dim_product.write.format("hudi") \
        .option("hoodie.table.name", "gold_dim_product") \
        .option("hoodie.datasource.write.recordkey.field", "HubProductID") \
        .option("hoodie.datasource.write.precombine.field", "HubProductID") \
        .mode("append") \
        .save(f"{base_gold}/dim_product")

    gold_dim_date.write.format("hudi") \
        .option("hoodie.table.name", "gold_dim_date") \
        .option("hoodie.datasource.write.recordkey.field", "HubDateID") \
        .option("hoodie.datasource.write.precombine.field", "HubDateID") \
        .mode("append") \
        .save(f"{base_gold}/dim_date")

def export_powerbi(spark):
    df = spark.read.format("hudi") \
        .load("/data/datalake/gold/fact_sales/*")

    df.coalesce(1) \
      .write \
      .mode("append") \
      .option("header", "true") \
      .csv("/data/datalake/gold/powerbi/fact_sales")

if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("AdventureWorksDW_DataVault_Hudi")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    load_bronze(spark)
    print("Bronze layer loaded.")

    load_silver(spark)
    print("Silver layer loaded.")

    load_gold(spark)
    print("Gold layer loaded.")

    export_powerbi(spark)

    spark.stop()