# PySpark — Zero to Advanced by Working With CSV Data

## How to use these notes

Do **not** just read the code.

For every section:

```text
Read explanation
    ↓
Run code
    ↓
Inspect output
    ↓
Change the code
    ↓
Solve the practice question
```

We use the accompanying:

```text
pyspark_sales_practice.csv
```

It contains 503 rows and deliberately includes data-quality problems.

---

# 1. What We Are Building

```text
Raw CSV
   ↓
Read with PySpark
   ↓
Inspect
   ↓
Find data-quality problems
   ↓
Clean
   ↓
Validate
   ↓
Transform
   ↓
Aggregate
   ↓
Join
   ↓
Window functions
   ↓
Optimize
   ↓
Write Parquet
```

This is much closer to how PySpark is used in a real ETL/data-engineering workflow.

---

# 2. Dataset

Columns:

```text
transaction_id
item
quantity
price_per_unit
total_spent
discount_pct
payment_method
city
transaction_date
```

The data intentionally contains:

```text
UNKNOWN
ERROR
blank values
whitespace
invalid numbers
negative values
invalid dates
duplicate rows
```

---

# 3. Install PySpark

```bash
pip install pyspark
```

Check:

```python
import pyspark

print(pyspark.__version__)
```

---

# 4. Start Spark

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("SalesETL")
    .master("local[*]")
    .getOrCreate()
)
```

`SparkSession` is the main entry point for working with Spark DataFrames and Spark SQL.

---

# 5. Read the CSV

Start with schema inference:

```python
df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("pyspark_sales_practice.csv")
)

df.show(10)
df.printSchema()
```

For production pipelines, explicit schemas are generally preferable.

---

# 6. Read With an Explicit Raw Schema

Keep raw fields as strings first so malformed source values remain visible.

```python
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType
)

schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("item", StringType(), True),
    StructField("quantity", StringType(), True),
    StructField("price_per_unit", StringType(), True),
    StructField("total_spent", StringType(), True),
    StructField("discount_pct", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("city", StringType(), True),
    StructField("transaction_date", StringType(), True)
])

df = (
    spark.read
    .option("header", True)
    .schema(schema)
    .csv("pyspark_sales_practice.csv")
)
```

---

# 7. Inspect the Data

```python
df.show(20, truncate=False)
```

Schema:

```python
df.printSchema()
```

Column names:

```python
print(df.columns)
```

Row count:

```python
print(df.count())
```

---

# 8. Select Columns

```python
df.select(
    "transaction_id",
    "item",
    "quantity"
).show()
```

Using `col()`:

```python
from pyspark.sql.functions import col

df.select(
    col("item"),
    col("quantity")
).show()
```

---

# 9. Filter Rows

```python
df.filter(
    col("city") == "Bhopal"
).show()
```

Multiple conditions:

```python
df.filter(
    (col("city") == "Bhopal") &
    (col("item") == "Laptop")
).show()
```

OR:

```python
df.filter(
    (col("city") == "Bhopal") |
    (col("city") == "Delhi")
).show()
```

`where()` is an alias for `filter()`:

```python
df.where(
    col("city") == "Delhi"
).show()
```

---

# 10. Find UNKNOWN and ERROR

```python
df.filter(
    col("item").isin(
        "UNKNOWN",
        "ERROR"
    )
).show()
```

Find blanks:

```python
df.filter(
    col("item") == ""
).show()
```

---

# 11. Count NULL Values

```python
from pyspark.sql.functions import sum as spark_sum

df.select([
    spark_sum(
        col(c).isNull().cast("int")
    ).alias(c)
    for c in df.columns
]).show()
```

---

# 12. Trim Whitespace

```python
from pyspark.sql.functions import trim

for c in [
    "item",
    "quantity",
    "price_per_unit",
    "total_spent",
    "discount_pct",
    "payment_method",
    "city",
    "transaction_date"
]:
    df = df.withColumn(
        c,
        trim(col(c))
    )
```

Why?

```text
" Bhopal "
```

and:

```text
"Bhopal"
```

should normally represent the same value.

---

# 13. Convert UNKNOWN / ERROR / Blank to NULL

```python
from pyspark.sql.functions import when

for c in [
    "item",
    "quantity",
    "price_per_unit",
    "total_spent",
    "payment_method",
    "city",
    "transaction_date"
]:
    df = df.withColumn(
        c,
        when(
            col(c).isin(
                "UNKNOWN",
                "ERROR",
                ""
            ),
            None
        ).otherwise(
            col(c)
        )
    )
```

Now:

```text
UNKNOWN → NULL
ERROR   → NULL
""      → NULL
```

---

# 14. Cast Quantity

```python
from pyspark.sql.types import IntegerType

df = df.withColumn(
    "quantity",
    col("quantity").cast(
        IntegerType()
    )
)
```

Check:

```python
df.printSchema()
```

---

# 15. Cast Decimal Columns

```python
from pyspark.sql.types import DecimalType

df = (
    df
    .withColumn(
        "price_per_unit",
        col("price_per_unit")
        .cast(DecimalType(10, 2))
    )
    .withColumn(
        "total_spent",
        col("total_spent")
        .cast(DecimalType(12, 2))
    )
    .withColumn(
        "discount_pct",
        col("discount_pct")
        .cast(DecimalType(5, 2))
    )
)
```

Invalid numeric strings can become NULL during casting.

---

# 16. Parse Dates

```python
from pyspark.sql.functions import to_date

df = df.withColumn(
    "transaction_date",
    to_date(
        "transaction_date",
        "yyyy-MM-dd"
    )
)
```

Check:

```python
df.printSchema()
```

---

# 17. Find Invalid Dates

```python
df.filter(
    col("transaction_date").isNull()
).show()
```

Remember:

```text
NULL after conversion
```

can mean the original value was missing or invalid.

For production pipelines, preserving raw data or adding validation flags can help distinguish these cases.

---

# 18. Find Invalid Quantities

```python
df.filter(
    col("quantity") <= 0
).show()
```

Valid business values:

```python
df.filter(
    col("quantity") > 0
).show()
```

---

# 19. Find Invalid Prices

```python
df.filter(
    col("price_per_unit") <= 0
).show()
```

---

# 20. Duplicate Rows

Exact duplicate count:

```python
duplicate_count = (
    df.count() -
    df.dropDuplicates().count()
)

print(duplicate_count)
```

Remove exact duplicates:

```python
df = df.dropDuplicates()
```

---

# 21. Duplicate Transaction IDs

```python
df.groupBy(
    "transaction_id"
).count().filter(
    col("count") > 1
).show()
```

If the transaction ID is genuinely the business key:

```python
df = df.dropDuplicates(
    ["transaction_id"]
)
```

Never deduplicate blindly. First determine what makes a record unique.

---

# 22. Create a Calculated Total

```python
df = df.withColumn(
    "calculated_total",
    col("quantity") *
    col("price_per_unit")
)
```

---

# 23. Include Discount

```python
df = df.withColumn(
    "calculated_total",
    col("quantity") *
    col("price_per_unit") *
    (
        1 -
        col("discount_pct") / 100
    )
)
```

Round:

```python
from pyspark.sql.functions import round

df = df.withColumn(
    "calculated_total",
    round(
        col("calculated_total"),
        2
    )
)
```

---

# 24. Validate the Source Total

```python
df.select(
    "transaction_id",
    "total_spent",
    "calculated_total"
).show()
```

Find mismatches:

```python
df.filter(
    col("total_spent") !=
    col("calculated_total")
).show()
```

This is an important ETL validation technique.

---

# 25. Create a Data-Quality Flag

```python
from pyspark.sql.functions import abs

df = df.withColumn(
    "total_check",
    when(
        abs(
            col("total_spent") -
            col("calculated_total")
        ) < 0.01,
        "VALID"
    ).otherwise(
        "MISMATCH"
    )
)
```

---

# 26. CASE WHEN

Create sales levels:

```python
df = df.withColumn(
    "sales_level",
    when(
        col("total_spent") >= 50000,
        "High"
    )
    .when(
        col("total_spent") >= 10000,
        "Medium"
    )
    .otherwise(
        "Low"
    )
)
```

Conceptually:

```text
PySpark when()
≈
SQL CASE WHEN
```

---

# 27. Rename Columns

```python
df = df.withColumnRenamed(
    "city",
    "customer_city"
)
```

---

# 28. Select Final Columns

```python
df = df.select(
    "transaction_id",
    "item",
    "quantity",
    "price_per_unit",
    "discount_pct",
    "total_spent",
    "payment_method",
    "customer_city",
    "transaction_date"
)
```

---

# 29. Sorting

Highest sales first:

```python
df.orderBy(
    col("total_spent").desc()
).show(10)
```

Lowest first:

```python
df.orderBy(
    col("total_spent").asc()
).show(10)
```

---

# 30. Aggregation

```python
from pyspark.sql.functions import (
    sum,
    avg,
    count,
    min,
    max
)

df.groupBy().agg(
    sum("total_spent").alias(
        "total_sales"
    ),
    avg("total_spent").alias(
        "average_order"
    ),
    count("*").alias(
        "transactions"
    )
).show()
```

---

# 31. Sales by City

```python
city_sales = (
    df.groupBy("customer_city")
      .agg(
          sum("total_spent")
          .alias("total_sales"),
          count("*")
          .alias("transactions"),
          avg("total_spent")
          .alias("average_order")
      )
      .orderBy(
          col("total_sales").desc()
      )
)

city_sales.show()
```

---

# 32. Sales by Product

```python
product_sales = (
    df.groupBy("item")
      .agg(
          sum("total_spent")
          .alias("sales"),
          count("*")
          .alias("transactions")
      )
      .orderBy(
          col("sales").desc()
      )
)

product_sales.show()
```

---

# 33. Top 10 Products

```python
product_sales.limit(10).show()
```

---

# 34. Payment Method Analysis

```python
payment_sales = (
    df.groupBy("payment_method")
      .agg(
          sum("total_spent")
          .alias("sales"),
          count("*")
          .alias("transactions")
      )
      .orderBy(
          col("sales").desc()
      )
)

payment_sales.show()
```

---

# 35. Date Features

```python
from pyspark.sql.functions import (
    year,
    month,
    date_format
)

df = (
    df
    .withColumn(
        "year",
        year("transaction_date")
    )
    .withColumn(
        "month",
        month("transaction_date")
    )
    .withColumn(
        "year_month",
        date_format(
            "transaction_date",
            "yyyy-MM"
        )
    )
)
```

---

# 36. Monthly Sales

```python
monthly_sales = (
    df.groupBy("year_month")
      .agg(
          sum("total_spent")
          .alias("sales"),
          count("*")
          .alias("transactions")
      )
      .orderBy("year_month")
)

monthly_sales.show()
```

---

# 37. Spark SQL

Register a temporary view:

```python
df.createOrReplaceTempView(
    "sales"
)
```

Then:

```python
result = spark.sql("""
    SELECT
        customer_city,
        SUM(total_spent) AS sales,
        COUNT(*) AS transactions
    FROM sales
    GROUP BY customer_city
    ORDER BY sales DESC
""")

result.show()
```

If you already know SQL, Spark SQL will feel familiar.

---

# 38. Create a Lookup Dataset

```python
city_data = [
    ("Bhopal", "Madhya Pradesh", "Central"),
    ("Delhi", "Delhi", "North"),
    ("Mumbai", "Maharashtra", "West"),
    ("Indore", "Madhya Pradesh", "Central"),
    ("Pune", "Maharashtra", "West")
]

city_df = spark.createDataFrame(
    city_data,
    ["customer_city", "state", "region"]
)
```

---

# 39. Left Join

```python
joined = df.join(
    city_df,
    on="customer_city",
    how="left"
)

joined.show()
```

---

# 40. Inner Join

```python
joined = df.join(
    city_df,
    on="customer_city",
    how="inner"
)

joined.show()
```

---

# 41. Left Anti Join

Find sales with no matching lookup record:

```python
unmatched = df.join(
    city_df,
    on="customer_city",
    how="left_anti"
)

unmatched.show()
```

This is very useful for data-quality validation.

---

# 42. Sales by Region

```python
region_sales = (
    joined.groupBy("region")
          .agg(
              sum("total_spent")
              .alias("sales"),
              count("*")
              .alias("transactions")
          )
          .orderBy(
              col("sales").desc()
          )
)

region_sales.show()
```

---

# 43. Window Functions

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import rank

city_product = (
    df.groupBy(
        "customer_city",
        "item"
    )
    .agg(
        sum("total_spent")
        .alias("sales")
    )
)

w = (
    Window
    .partitionBy("customer_city")
    .orderBy(
        col("sales").desc()
    )
)

ranked = city_product.withColumn(
    "rank",
    rank().over(w)
)

ranked.show()
```

---

# 44. Top 3 Products Per City

```python
ranked.filter(
    col("rank") <= 3
).show()
```

This is a very common analytics interview problem.

---

# 45. `row_number()`

```python
from pyspark.sql.functions import row_number

ranked = city_product.withColumn(
    "row_number",
    row_number().over(w)
)
```

Difference:

```text
rank()
→ ties can have the same rank

row_number()
→ every row receives a unique number
```

---

# 46. `dense_rank()`

```python
from pyspark.sql.functions import dense_rank

ranked = city_product.withColumn(
    "dense_rank",
    dense_rank().over(w)
)
```

---

# 47. `lag()`

Look at the previous sale:

```python
from pyspark.sql.functions import lag

w = (
    Window
    .partitionBy("customer_city")
    .orderBy("transaction_date")
)

df = df.withColumn(
    "previous_sale",
    lag("total_spent").over(w)
)
```

---

# 48. Compare With Previous Sale

```python
df = df.withColumn(
    "sale_change",
    col("total_spent") -
    col("previous_sale")
)
```

---

# 49. Running Total

```python
w = (
    Window
    .partitionBy("customer_city")
    .orderBy("transaction_date")
    .rowsBetween(
        Window.unboundedPreceding,
        Window.currentRow
    )
)

df = df.withColumn(
    "running_sales",
    sum("total_spent").over(w)
)
```

---

# 50. Partitions

Check:

```python
print(
    df.rdd.getNumPartitions()
)
```

Conceptually:

```text
Data
 ↓
Partition 1
Partition 2
Partition 3
Partition 4
 ↓
Tasks
 ↓
Executors
```

Spark processes partitions in parallel.

---

# 51. Repartition

```python
df = df.repartition(8)
```

This generally causes a shuffle.

Use it intentionally.

---

# 52. Coalesce

```python
df = df.coalesce(4)
```

Useful when reducing partitions.

Avoid:

```python
df.coalesce(1)
```

for large production datasets merely to force one output file.

---

# 53. What Is a Shuffle?

Operations such as:

```text
groupBy
join
distinct
orderBy
repartition
```

may require records to move between partitions.

That movement is called:

```text
SHUFFLE
```

Shuffles can be expensive.

---

# 54. Lazy Evaluation

This does not immediately execute the full computation:

```python
filtered = df.filter(
    col("total_spent") > 1000
)
```

Spark builds a logical plan.

An action triggers execution:

```python
filtered.show()
```

Transformations:

```text
select
filter
where
withColumn
join
groupBy
orderBy
```

Actions:

```text
show
count
collect
write
```

---

# 55. `explain()`

```python
df.explain()
```

Better:

```python
df.explain(
    "formatted"
)
```

Learn to recognize:

```text
Scan
Filter
Project
Exchange
Aggregate
Sort
Join
```

`Exchange` commonly indicates a shuffle boundary.

---

# 56. Avoid `collect()` on Large Data

Avoid:

```python
rows = df.collect()
```

on large datasets.

It brings data to the driver.

Use:

```python
df.show()
```

or:

```python
df.limit(100).collect()
```

when the result is intentionally small.

---

# 57. Cache

If an expensive DataFrame is reused:

```python
df.cache()

df.count()
```

Then reuse:

```python
df.show()
```

When finished:

```python
df.unpersist()
```

Do not cache everything.

---

# 58. Broadcast Join

If the lookup table is genuinely small:

```python
from pyspark.sql.functions import broadcast

joined = df.join(
    broadcast(city_df),
    on="customer_city",
    how="left"
)
```

This can avoid a large shuffle.

Do not broadcast large datasets.

---

# 59. Write Parquet

```python
df.write.mode(
    "overwrite"
).parquet(
    "output/clean_sales"
)
```

Read:

```python
clean_df = spark.read.parquet(
    "output/clean_sales"
)
```

Parquet is usually much more appropriate than CSV for analytical storage.

---

# 60. Why Parquet?

CSV:

```text
text
weak typing
row-oriented
larger files
less efficient analytical reads
```

Parquet:

```text
columnar
typed
compressed
column pruning
predicate pushdown
analytics-friendly
```

---

# 61. Partitioned Parquet

```python
df.write.mode(
    "overwrite"
).partitionBy(
    "year"
).parquet(
    "output/sales_by_year"
)
```

Choose partition columns carefully.

Avoid very high-cardinality partition columns because they can create many tiny files.

---

# 62. Write CSV

```python
df.write.mode(
    "overwrite"
).option(
    "header",
    True
).csv(
    "output/clean_sales_csv"
)
```

Spark normally creates a directory containing part files.

---

# 63. Bronze / Silver / Gold

A common data-engineering architecture:

```text
RAW CSV
   ↓
BRONZE
   ↓
SILVER
   ↓
GOLD
```

## Bronze

Raw source data.

Minimal transformations.

## Silver

Cleaned and standardized:

```text
NULL handling
type casting
date parsing
deduplication
validation
```

## Gold

Business-ready:

```text
monthly sales
product sales
city sales
region sales
payment sales
```

---

# 64. Complete Project Architecture

```text
project/
│
├── data/
│   └── raw/
│       └── pyspark_sales_practice.csv
│
├── src/
│   ├── bronze.py
│   ├── silver.py
│   └── gold.py
│
├── output/
│   ├── bronze/
│   ├── silver/
│   └── gold/
│
└── README.md
```

---

# 65. Gold Product Table

```python
product_sales = (
    df.groupBy("item")
      .agg(
          sum("total_spent")
          .alias("total_sales"),
          count("*")
          .alias("transactions"),
          avg("total_spent")
          .alias("average_order")
      )
)
```

---

# 66. Gold City Table

```python
city_sales = (
    df.groupBy("customer_city")
      .agg(
          sum("total_spent")
          .alias("total_sales"),
          count("*")
          .alias("transactions"),
          avg("total_spent")
          .alias("average_order")
      )
)
```

---

# 67. Gold Payment Table

```python
payment_sales = (
    df.groupBy("payment_method")
      .agg(
          sum("total_spent")
          .alias("total_sales"),
          count("*")
          .alias("transactions")
      )
)
```

---

# 68. Save Gold Tables

```python
product_sales.write.mode(
    "overwrite"
).parquet(
    "output/gold/product_sales"
)

city_sales.write.mode(
    "overwrite"
).parquet(
    "output/gold/city_sales"
)

payment_sales.write.mode(
    "overwrite"
).parquet(
    "output/gold/payment_sales"
)
```

---

# 69. Practice — Beginner

Using the CSV:

1. Load the dataset.
2. Display 20 rows.
3. Print the schema.
4. Count rows.
5. Select `item`, `quantity`, and `total_spent`.
6. Find UNKNOWN items.
7. Find ERROR values.
8. Find blank cities.
9. Trim whitespace.
10. Count NULL values in every column.

---

# 70. Practice — Data Cleaning

11. Convert UNKNOWN/ERROR/blank values to NULL.
12. Cast quantity to integer.
13. Cast prices to decimal.
14. Cast discount to decimal.
15. Parse the transaction date.
16. Find invalid dates.
17. Find quantity <= 0.
18. Find price <= 0.
19. Find duplicate transaction IDs.
20. Remove duplicates.
21. Calculate expected revenue.
22. Compare expected and source totals.
23. Create a validation flag.

---

# 71. Practice — Transformations

24. Create `sales_level`.
25. Extract year.
26. Extract month.
27. Create `year_month`.
28. Sort products by sales.
29. Find the top 10 products.
30. Find the top 5 cities.
31. Calculate average transaction value.
32. Calculate monthly sales.

---

# 72. Practice — SQL

33. Register the DataFrame as a temporary view.
34. Calculate city sales using Spark SQL.
35. Calculate product sales using Spark SQL.
36. Find the highest-sales city.
37. Find the top 5 products using SQL.

---

# 73. Practice — Joins

38. Create the city lookup DataFrame.
39. Perform a left join.
40. Perform an inner join.
41. Find unmatched cities with `left_anti`.
42. Calculate sales by region.

---

# 74. Practice — Window Functions

43. Rank products by sales.
44. Rank products within each city.
45. Find top 3 products per city.
46. Use `row_number()`.
47. Use `dense_rank()`.
48. Use `lag()`.
49. Calculate change from previous sale.
50. Calculate a running total.

---

# 75. Practice — Performance

51. Check partition count.
52. Run `explain("formatted")`.
53. Find `Exchange`.
54. Compare repartition and coalesce.
55. Test a broadcast join.
56. Cache a reused DataFrame.
57. Write the final dataset to Parquet.
58. Partition Parquet by year.

---

# 76. Final Challenge

Build the entire pipeline yourself:

```text
pyspark_sales_practice.csv
          ↓
        BRONZE
          ↓
     raw Parquet
          ↓
        SILVER
          ↓
clean + typed + validated
          ↓
         GOLD
          ↓
monthly_sales
product_sales
city_sales
payment_sales
```

Your final project should have:

```text
1. Input
2. Raw ingestion
3. Data-quality checks
4. Cleaning
5. Type conversion
6. Validation
7. Deduplication
8. Transformations
9. Aggregations
10. Joins
11. Window functions
12. Parquet output
13. Performance inspection
```

---

# 77. PySpark Functions to Memorize

### Column

```python
col()
lit()
```

### Conditional

```python
when()
```

### String

```python
trim()
lower()
upper()
length()
regexp_replace()
```

### Numeric

```python
round()
abs()
```

### Dates

```python
to_date()
year()
month()
date_format()
datediff()
```

### Aggregation

```python
sum()
avg()
count()
min()
max()
countDistinct()
```

### Windows

```python
row_number()
rank()
dense_rank()
lag()
lead()
```

---

# 78. PySpark vs Pandas

You already know Pandas, so remember:

```text
Pandas
→ primarily in-memory, single-machine analysis

PySpark
→ distributed processing across partitions/executors
```

Conceptually:

```python
# Pandas
df[df["sales"] > 1000]
```

PySpark:

```python
df.filter(
    col("sales") > 1000
)
```

But do not think:

```text
PySpark = Pandas for bigger files
```

The execution model is fundamentally different.

---

# 79. The Most Important Spark Concepts

You should eventually be able to explain:

```text
Spark
PySpark
SparkSession
DataFrame
Schema
Driver
Executor
Task
Stage
Partition
Transformation
Action
Lazy evaluation
Shuffle
Exchange
Repartition
Coalesce
Broadcast join
Window function
Cache
Parquet
Column pruning
Predicate pushdown
Catalyst optimizer
```

---

# 80. Final Mental Model

Do not learn PySpark as just syntax.

Think:

```text
Your PySpark code
       ↓
Logical Plan
       ↓
Catalyst Optimizer
       ↓
Physical Plan
       ↓
Stages
       ↓
Tasks
       ↓
Partitions
       ↓
Executors
```

That is the foundation of real Spark knowledge.

---

# 81. Learning Order

Follow this order:

```text
P1  Read CSV
 ↓
P2  Inspect
 ↓
P3  Select / Filter
 ↓
P4  Clean strings
 ↓
P5  NULL handling
 ↓
P6  Type casting
 ↓
P7  Date handling
 ↓
P8  Duplicate handling
 ↓
P9  withColumn
 ↓
P10 Validation
 ↓
P11 Aggregations
 ↓
P12 Date analytics
 ↓
P13 Spark SQL
 ↓
P14 Joins
 ↓
P15 Window functions
 ↓
P16 Parquet
 ↓
P17 Partitions
 ↓
P18 Shuffle
 ↓
P19 explain()
 ↓
P20 Cache / Broadcast
 ↓
P21 Bronze / Silver / Gold
 ↓
P22 Complete ETL project
```

## Golden Rule

**Do not move to the next section until you have run the current section against the CSV and understand the output.**
