def model (dbt,session):
    stg_customer_df=dbt.source("qwt_raw","customers")
    return stg_customer_df
