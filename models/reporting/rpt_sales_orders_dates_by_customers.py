import snowflake.snowpark.functions as f
import pandas as pd
import holidays

def avgsale(x,y):
    return y/x

def is_holiday(date_col):
    french_holidays= holidays.France()
    is_holiday=(date_col in french_holidays)
    return is_holiday

def model(dbt, session):
    dbt.config (materialized='table', schema='reporting',packages=["holidays"])
    dim_customers_df=dbt.ref('dim_customer')

    fct_orders_df=dbt.ref('fct_orders')
    customers_order_df= (
                        fct_orders_df
                        .group_by('customerid')
                        .agg
                       (
                        f.min(f.col('orderdate')).alias ('first_orderdate'),
                        f.max (f.col('orderdate')).alias ('last_orderdate'),
                        f.count(f.col('orderid')).alias ('total_orders') ,
                        f.sum(f.col('linesaleamount')).alias('total_sales'),
                        f.avg(f.col('margin')).alias ('avg_margin')
                       )
                     )
    final_df = (
                    dim_customers_df
                    .join(customers_order_df, customers_order_df.customerid == dim_customers_df.customer_id, 'left')
                    .select(
                    dim_customers_df.company_name.alias('companyanme'),
                    dim_customers_df.contact_name.alias('contactname'),
                    customers_order_df.first_orderdate.alias('first_orderdate'),
                    customers_order_df.last_orderdate.alias('recent_orderdate'),
                    customers_order_df.total_orders.alias('total_orders'),
                    customers_order_df.total_sales.alias('total_sales'),
                    customers_order_df.avg_margin.alias('avg_margin')
                    )
                )
    final_df=final_df.withColumn("avg_salevalue",avgsale(final_df["total_orders"],final_df["total_sales"]))
    final_df=final_df. filter(f.col("first_orderdate").isNotNull())
    final_df=final_df.to_pandas() #CONVERTING TO Pandas#
    final_df["IS_FIRST_ORDERDATE_HOLIDAY"] = final_df["FIRST_ORDERDATE"].apply(is_holiday)
    return final_df


    