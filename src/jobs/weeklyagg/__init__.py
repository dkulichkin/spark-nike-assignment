import json
import os
from pyspark.sql.types import *
import pyspark.sql.functions as sf


def get_weekly_data_row(dictionary):
    return {"W" + str(w): dictionary["W" + str(w)] for w in range(1, 53)}


def reduce_by_key_and_sum(default):
    def func(row):
        res = { "W" + str(w): default for w in range(1, 53) }
        for e in row:
            (key, value) = e
            key = "W" + str(key)
            if key in res:
                res[key] += value
        return res
    return func


def extract(spark, base_path="./resources"):
    sdf_sales = (
        spark.read.format("csv").option("header", "true")
        .option("ignoreLeadingWhiteSpace", "true")
        .option("ignoreTrailingWhiteSpace", "true")
        .schema(StructType([
            StructField("saleId", IntegerType(), False),
            StructField("netSales", FloatType(), False),
            StructField("salesUnits", IntegerType(), False),
            StructField("storeId", IntegerType(), False),
            StructField("dateId", IntegerType(), False),
            StructField("productId", LongType(), False)
        ]))
        .load(os.path.join(base_path, "sales.csv"))
    )

    sdf_calendar = (
        spark.read.format("csv").option("header", "true")
        .option("ignoreLeadingWhiteSpace", "true")
        .option("ignoreTrailingWhiteSpace", "true")
        .schema(StructType([
            StructField("datekey", IntegerType(), False),
            StructField("datecalendarday", IntegerType(), False),
            StructField("datecalendaryear", IntegerType(), False),
            StructField("weeknumberofseason", IntegerType(), False)
        ]))
        .load(os.path.join(base_path, "calendar.csv"))
    )

    sdf_product = (
        spark.read.format("csv").option("header", "true")
        .option("ignoreLeadingWhiteSpace", "true")
        .option("ignoreTrailingWhiteSpace", "true")
        .schema(StructType([
            StructField("productid", LongType(), False),
            StructField("division", StringType(), False),
            StructField("gender", StringType(), False),
            StructField("category", StringType(), False)
        ]))
        .load(os.path.join(base_path, "product.csv"))
    )

    sdf_store = (
        spark.read.format("csv").option("header", "true")
        .option("ignoreLeadingWhiteSpace", "true")
        .option("ignoreTrailingWhiteSpace", "true")
        .schema(StructType([
            StructField("storeid", IntegerType(), False),
            StructField("channel", StringType(), False),
            StructField("country", StringType(), False)
        ]))
        .load(os.path.join(base_path, "store.csv"))
    )

    return sdf_sales \
        .join(sdf_calendar, sf.col("dateId") == sf.col("datekey")) \
        .join(sdf_product.withColumnRenamed("productid", "productkey"), sf.col("productId") == sf.col("productid")) \
        .join(sdf_store.withColumnRenamed("storeid", "storekey"), sf.col("storeId") == sf.col("storeid")) \
        .drop('dateId', 'datekey', 'productId', 'productkey', 'storeId', 'storekey')


def transform(sdf):
    net_sales_by_week_udf = sf.udf(reduce_by_key_and_sum(0.0), MapType(StringType(), FloatType()))
    sales_units_by_week_udf = sf.udf(reduce_by_key_and_sum(0), MapType(StringType(), IntegerType()))

    return sdf.groupBy("datecalendaryear", "channel", "division", "gender", "category") \
        .agg(
            net_sales_by_week_udf(sf.collect_list(sf.struct("weeknumberofseason", "netSales"))).alias("netsalesbyweek"),
            sales_units_by_week_udf(sf.collect_list(sf.struct("weeknumberofseason", "salesUnits"))).alias("salesunitsbyweek")
        )


def load(sdf, base_path="./results"):
    for row in sdf.collect():
        year = f"RY{str(row['datecalendaryear'])[2:]}"
        unique_key = f"{year}_{row['channel']}_{row['division']}_{row['gender']}_{row['category']}"
        file_name = unique_key + ".json"
        with open(os.path.join(base_path, file_name), 'w') as file:
            res = {
                "uniqueKey": unique_key,
                "division": row["division"],
                "gender": row["gender"],
                "category": row["category"],
                "channel": row["channel"],
                "year": year,
                "dataRows": [
                    {
                        "rowId": "Net Sales",
                        "dataRow": get_weekly_data_row(row["netsalesbyweek"])
                    },
                    {
                        "rowId": "Sales Units",
                        "dataRow": get_weekly_data_row(row["salesunitsbyweek"])
                    }
                ]
            }
            json.dump(res, file, indent=4)
            print(f"Succeeded creating {file_name}")