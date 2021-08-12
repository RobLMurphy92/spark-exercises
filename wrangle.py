from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pandas as pd
from pyspark.sql.functions import col, expr
from pyspark.sql.functions import concat, sum, avg, min, max, count, mean
from pyspark.sql.functions import lit
from pyspark.sql.functions import regexp_extract, regexp_replace
from pyspark.sql.functions import when
from pyspark.sql.functions import asc, desc
from pyspark.sql.functions import month, year, quarter


from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType

spark = SparkSession.builder.getOrCreate()






def wrangle_data():
    schema = StructType(
        [
            StructField("source_id", StringType()),
            StructField("source_username", StringType()),
        ]
    )
    schema1 = StructType(
        [
            StructField("case_id", StringType()),
            StructField("case_opened_date", StringType()),
            StructField("case_closed_date", StringType()),
            StructField("SLA_due_date", StringType()),
            StructField("case_late", StringType()),
            StructField("num_days_late", StringType()),
            StructField("case_closed", StringType()),
            StructField("dept_division", StringType()),
            StructField("service_request_type", StringType()),
            StructField("SLA_days", StringType()),
            StructField("case_status", StringType()),
            StructField("source_id", StringType()),
            StructField("request_address", StringType()),
            StructField("council_district", StringType()),
        
        ]
    )

    schema2 = StructType(
        [
            StructField("dept_division", StringType()),
            StructField("dept_name", StringType()),
            StructField("standardized_dept_name", StringType()),
            StructField("dept_subject_to_SLA", StringType())
        ]
    )
    
    source = spark.read.csv("source.csv", header=True, schema=schema)
    case = spark.read.csv("case.csv", header=True, schema=schema1)
    dept = spark.read.csv("dept.csv", header=True, schema=schema2)
    
    df = case.join(dept, on=dept.dept_division == case.dept_division).drop(dept.dept_division)
    df = df.join(source, on = df.source_id== source.source_id).drop(source.source_id)
    
    df = df.withColumnRenamed("SLA_due_date", "case_due_date")
    df = df.withColumn("case_closed", expr('case_closed == "YES"')).withColumn(
    "case_late", expr('case_late == "YES"'))
    df = df.withColumn("council_district", col("council_district").cast("string"))
    fmt = "M/d/yy H:mm"

    df = df.withColumn("case_opened_date", to_timestamp("case_opened_date", fmt)).withColumn("case_closed_date", to_timestamp("case_opened_date",fmt)).withColumn("case_due_date", to_timestamp("case_opened_date", fmt))
    df = df.withColumn("request_address", trim(lower(df.request_address)))
    df = df.withColumn("num_weeks_late", expr("num_days_late / 7 AS num_weeks_late"))
    df = df.withColumn("council_district", col("council_district").cast("int"))
    df = df.withColumn("council_district",format_string("%03d", col("council_district").cast("int")),)
    df = df.withColumn("zipcode", regexp_extract("request_address", r"\d+$", 0))
    df = (df.withColumn("case_age", datediff(current_timestamp(), "case_opened_date")).withColumn("days_to_closed", datediff("case_closed_date", "case_opened_date"))
    .withColumn("case_lifetime", when(expr("! case_closed"), col("case_age")).otherwise(col("days_to_closed")),))
    return df