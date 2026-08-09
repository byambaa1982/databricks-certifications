# DataFrame transformations

Bronze-to-silver is where most transformation questions live: casting strings,
handling bad rows, conditional logic, joins, and aggregation.

## Setup

```python
CATALOG, SCHEMA = "dev_catalog", "dev_byambaa1982_lakehouse_starter"
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

from pyspark.sql import functions as F
```

## Casting and cleaning (what silver actually does to bronze)

`bronze_orders` stores `order_ts` as STRING and `amount` as DOUBLE. Silver
needs proper types:

```python
orders = (
    spark.table("bronze_orders")
    .withColumn("order_ts", F.to_timestamp("order_ts"))          # bad strings -> NULL, no error
    .withColumn("amount", F.col("amount").cast("decimal(10,2)")) # money is DECIMAL, never DOUBLE
    .withColumn("order_date", F.to_date("order_ts"))
    .withColumn("status", F.lower(F.trim("status")))
)
orders.select("order_id", "order_ts", "amount", "status").show(5)
```

> **Exam trap:** `cast()` and `to_timestamp()` return **NULL** on unparseable
> input — they never throw. If a question implies a job *fails* on a bad cast,
> that's wrong unless ANSI mode (`spark.sql.ansi.enabled = true`) is on, where
> invalid casts DO raise errors.

## Conditional logic: `when` / `otherwise`

```python
(orders
 .withColumn("order_size",
     F.when(F.col("amount") >= 500, "large")
      .when(F.col("amount") >= 100, "medium")
      .otherwise("small"))
 .groupBy("order_size").count().show())
```

Without `.otherwise()`, unmatched rows get **NULL** — a favorite exam question.

## selectExpr / SQL expressions

Equivalent to the above, exam loves showing both syntaxes:

```python
spark.table("bronze_orders").selectExpr(
    "order_id",
    "cast(amount as decimal(10,2)) as amount",
    "to_timestamp(order_ts) as order_ts",
    "case when status = 'completed' then amount else 0 end as recognized_revenue",
).show(5)
```

## Joins — enriching orders with the current customer tier

`silver_customers` is SCD Type 2: **filter to current rows first**, or the join
fans out (one order row per historical version of the customer):

```python
current_customers = (
    spark.table("silver_customers")
    .filter(F.col("__END_AT").isNull())          # current version only
    .select("customer_id", "name", "tier")
)

enriched = spark.table("silver_orders").join(current_customers, "customer_id", "left")
enriched.groupBy("tier").agg(
    F.sum("amount").alias("revenue"),
    F.countDistinct("customer_id").alias("customers"),
).show()
```

> **Exam trap:** joining a fact table to an SCD2 dimension without filtering
> `__END_AT IS NULL` (or range-joining on the validity window) **duplicates
> fact rows**. Both the bug and both fixes show up in questions.

Point-in-time join (the "correct at order time" variant):

```sql
SELECT o.order_id, o.amount, c.tier AS tier_at_order_time
FROM silver_orders o
JOIN silver_customers c
  ON o.customer_id = c.customer_id
 AND o.order_ts >= to_timestamp(c.__START_AT)
 AND (c.__END_AT IS NULL OR o.order_ts < to_timestamp(c.__END_AT));
```

## Aggregations and the null-handling rules

```sql
SELECT
  status,
  count(*)          AS all_rows,       -- counts NULLs
  count(amount)     AS non_null_amts,  -- skips NULLs
  sum(amount)       AS revenue,        -- skips NULLs; NULL if ALL inputs null
  avg(amount)       AS avg_amount      -- divides by non-null count only
FROM bronze_orders
GROUP BY status;
```

## Narrow vs wide transformations

- **Narrow** (no shuffle): `select`, `filter`, `withColumn`, `union`
- **Wide** (shuffle): `groupBy`, `join`, `distinct`, `orderBy`, `repartition`

`coalesce(n)` reduces partitions **without** a shuffle; `repartition(n)` always
shuffles (and can increase partitions, which `coalesce` cannot).

## Verify your understanding

Recompute `gold_daily_revenue` from `silver_orders` and diff it against the
published table — zero rows means your aggregation logic matches the pipeline's:

```sql
WITH mine AS (
  SELECT order_date,
         coalesce(sum(CASE WHEN status = 'completed' THEN amount END),
                  cast(0 AS decimal(18,2)))                    AS revenue,
         count(*)                                              AS order_count,
         sum(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END) AS cancelled_count
  FROM silver_orders GROUP BY order_date
)
SELECT * FROM mine
EXCEPT
SELECT order_date, revenue, order_count, cancelled_count FROM gold_daily_revenue;
```

Note the `coalesce(..., 0)`: a day with rows but zero completed orders would
otherwise produce NULL revenue — the pipeline (see `transforms.daily_revenue`)
makes the same choice, and `sum(CASE ...)` returning NULL when every branch is
NULL is itself a recurring exam question.
```sql
```
