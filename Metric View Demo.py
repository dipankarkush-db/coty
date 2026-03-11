# Databricks notebook source
# MAGIC %sql
# MAGIC use catalog dkushari_uc;
# MAGIC create schema if not exists dkushari_uc.metric_view_demo;
# MAGIC use dkushari_uc.metric_view_demo;

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists customer deep clone samples.tpch.customer;
# MAGIC create table if not exists orders deep clone samples.tpch.orders;
# MAGIC create table if not exists lineitem deep clone samples.tpch.lineitem;
# MAGIC create table if not exists part deep clone samples.tpch.part;
# MAGIC create table if not exists supplier deep clone samples.tpch.supplier;
# MAGIC create table if not exists partsupp deep clone samples.tpch.partsupp;
# MAGIC create table if not exists nation deep clone samples.tpch.nation;
# MAGIC create table if not exists region deep clone samples.tpch.region;

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists metric_view_schema;

# COMMAND ----------

# MAGIC %sql
# MAGIC use dkushari_uc.metric_view_schema;
# MAGIC DROP VIEW IF EXISTS demo_orders_metric_view;
# MAGIC DROP VIEW IF EXISTS demo_lineitem_metric_view;

# COMMAND ----------

# version: 1.1

# source: dkushari_uc.metric_view_demo.orders

# filter: o_orderdate > '1990-01-01'

# dimensions:
#   - name: Order Month
#     expr: "date_format(o_orderdate, 'MMMM')"
#   - name: Order Month Date
#     expr: "DATE_TRUNC('MONTH', o_orderdate)"
#     format:
#       type: date
#       date_format: locale_short_month
#   - name: Order Status
#     expr: |-
#       case
#         when o_orderstatus = 'O' then 'Open'
#         when o_orderstatus = 'P' then 'Processing'
#         when o_orderstatus = 'F' then 'Fulfilled'
#       end
#   - name: Order Priority
#     expr: "split(o_orderpriority, '-')[1]"

# measures:
#   - name: Order Count
#     expr: count(1)
#   - name: Total Revenue
#     expr: SUM(o_totalprice)
#   - name: Total Revenue per Customer
#     expr: SUM(o_totalprice) / count(distinct o_custkey)
#   - name: Total Revenue for Open Orders
#     expr: SUM(o_totalprice) filter (where o_orderstatus='O')
#   - name: Total Revenue Prior Month
#     expr: SUM(o_totalprice)
#     window:
#       - order: Order Month Date
#         semiadditive: last
#         range: trailing 1 month
#   - name: Revenue MoM Difference
#     expr: MEASURE(`Total Revenue`) - MEASURE(`Total Revenue Prior Month`)

# COMMAND ----------

# MAGIC %sql
# MAGIC use dkushari_uc.metric_view_schema;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe table demo_orders_metric_view;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE EXTENDED demo_orders_metric_view;

# COMMAND ----------

# MAGIC %md
# MAGIC ###Evaluate 3 listed measures, using the metric view definition, and aggregate over Order Month and Order Status and sort by Order Month.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     `Order Month Date` as current_month,
# MAGIC     `Order Status` as order_status,
# MAGIC     MEASURE(`Order Count`) as order_count,
# MAGIC     MEASURE(`Total Revenue`) as total_revenue,
# MAGIC     MEASURE(`Total Revenue Prior Month`) as total_revenue_prior_month,
# MAGIC     MEASURE(`Total Revenue per Customer`) as total_revenue_per_customer,
# MAGIC     MEASURE(`Revenue MoM Difference`) as revenue_mom_difference
# MAGIC   FROM
# MAGIC     demo_orders_metric_view
# MAGIC   WHERE date_format(`Order Month Date`, 'MMMM') = :month
# MAGIC   GROUP BY ALL
# MAGIC   ORDER BY `Order Month Date` ASC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     `Order Month Date`,
# MAGIC     `Order Status`,
# MAGIC     MEASURE(`Order Count`) as order_count,
# MAGIC     MEASURE(`Total Revenue`) as total_revenue,
# MAGIC     MEASURE(`Total Revenue Prior Month`) as total_revenue_prior_month,
# MAGIC     MEASURE(`Total Revenue per Customer`) as total_revenue_per_customer,
# MAGIC     MEASURE(`Revenue MoM Difference`) as revenue_mom_difference
# MAGIC   FROM
# MAGIC     demo_orders_metric_view
# MAGIC   WHERE date_format(`Order Month Date`, 'MMMM') = :month
# MAGIC   GROUP BY ALL
# MAGIC   ORDER BY `Order Month Date` ASC
# MAGIC  

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC  `Order Priority`,
# MAGIC  MEASURE(`Order Count`) as order_count,
# MAGIC  MEASURE(`Total Revenue`) as total_revenue,
# MAGIC  MEASURE(`Total Revenue per Customer`) as total_revenue_per_customer
# MAGIC FROM
# MAGIC  demo_orders_metric_view
# MAGIC GROUP BY `Order Priority`
# MAGIC ORDER BY 1 ASC
# MAGIC limit 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC use dkushari_uc.metric_view_demo;
# MAGIC SELECT orders.o_orderkey,
# MAGIC        orders.o_orderdate, 
# MAGIC        sum(l_quantity) as total_quantity, 
# MAGIC        sum(l_extendedprice) price
# MAGIC FROM lineitem
# MAGIC JOIN orders ON lineitem.l_orderkey = orders.o_orderkey
# MAGIC group by ALL
# MAGIC order by orders.o_orderkey
# MAGIC limit 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC USE dkushari_uc.metric_view_schema;
# MAGIC DROP VIEW IF EXISTS demo_lineitem_metric_view;
# MAGIC CREATE VIEW `demo_lineitem_metric_view` 
# MAGIC (
# MAGIC  `order_date` COMMENT "The order date",
# MAGIC  `order_key` COMMENT "Order key", 
# MAGIC  `total_quantity` COMMENT "Total quantity", 
# MAGIC  `total_price` COMMENT "Total price"
# MAGIC  )  
# MAGIC WITH METRICS
# MAGIC LANGUAGE YAML 
# MAGIC AS $$ 
# MAGIC version: 1.1
# MAGIC
# MAGIC source: select * from dkushari_uc.metric_view_demo_setup.lineitem
# MAGIC
# MAGIC joins:
# MAGIC  - name: orders
# MAGIC    source: select * from dkushari_uc.metric_view_demo_setup.orders
# MAGIC    on: o_orderkey = l_orderkey
# MAGIC
# MAGIC dimensions:
# MAGIC  - name: order_date
# MAGIC    expr: orders.o_orderdate 
# MAGIC  - name: order_key
# MAGIC    expr: orders.o_orderkey 
# MAGIC
# MAGIC measures:
# MAGIC  - name: total_quantity
# MAGIC    expr: sum(l_quantity)
# MAGIC  - name: total_price
# MAGIC    expr: sum(l_extendedprice)
# MAGIC $$
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE EXTENDED demo_lineitem_metric_view;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE EXTENDED demo_lineitem_metric_view AS JSON;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW VIEWS IN dkushari_uc.metric_view_schema;

# COMMAND ----------

# MAGIC %sql
# MAGIC use dkushari_uc.metric_view_schema;
# MAGIC SELECT order_key,
# MAGIC        order_date,
# MAGIC        MEASURE(total_quantity),
# MAGIC        MEASURE(total_price)
# MAGIC FROM demo_lineitem_metric_view
# MAGIC group by ALL
# MAGIC order by order_key
# MAGIC limit 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC use dkushari_uc.metric_view_schema;
# MAGIC ALTER VIEW `demo_lineitem_metric_view` rename to `lineitem_order_metric_view`;

# COMMAND ----------

# MAGIC %sql
# MAGIC show views in dkushari_uc.metric_view_schema;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE EXTENDED lineitem_order_metric_view;