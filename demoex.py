import dlt
from pyspark.sql.functions import col, regexp_replace, regexp_extract, avg, count

@dlt.table(
  name="cars_bronze",
  comment="Raw cars data used"
)
def bronze():
    df = (
        spark.read.option("header", True).csv("/Volumes/workspace/demo1/cars/Cars_Datasets_2025.csv")
    )

    return (
        df.withColumnRenamed("Company Names", "company_name")
          .withColumnRenamed("Cars Names", "car_name")
          .withColumnRenamed("Engines", "engine")
          .withColumnRenamed("CC/Battery Capacity", "cc_or_battery")
          .withColumnRenamed("HorsePower", "horsepower")
          .withColumnRenamed("Total Speed", "total_speed")
          .withColumnRenamed("Performance(0 - 100 )KM/H", "acceleration")
          .withColumnRenamed("Cars Prices", "car_price")
          .withColumnRenamed("Fuel Types", "fuel_type")
          .withColumnRenamed("Seats", "seats")
          .withColumnRenamed("Torque", "torque")
    )

def extract_number(col_name):
    cleaned = regexp_replace(col(col_name), "[^0-9.]", "")  
    return regexp_extract(cleaned, r"([0-9]+\.?[0-9]*)", 1).cast("double")
@dlt.table(
  name="cars_silver",
  comment="Cleaned cars data with cleaned numeric values"
)
@dlt.expect("valid_price", "car_price > 100000")
def silver():
    df = dlt.read("cars_bronze")

    return (
        df.dropna()
          .withColumn("cc_or_battery", extract_number("cc_or_battery"))
          .withColumn("horsepower", extract_number("horsepower"))
          .withColumn("total_speed", extract_number("total_speed"))
          .withColumn("acceleration", extract_number("acceleration"))
          .withColumn("car_price", extract_number("car_price"))
          .withColumn("torque_nm", extract_number("torque"))
          .withColumn("seats", col("seats").cast("int"))
          .filter(col("car_price").isNotNull())
    )

@dlt.table(
  name="cars_gold",
  comment="Insights by fuel type"
)
def gold():
    df = dlt.read("cars_silver")
    return (
        df.groupBy("fuel_type")
          .agg(
              avg("car_price").alias("avg_price"),
              avg("horsepower").alias("avg_horsepower"),
              avg("torque_nm").alias("avg_torque"),
              avg("acceleration").alias("avg_acceleration"),
              count("*").alias("total_models")
          )
          .orderBy(col("avg_price").desc())
    )
