from flask import Flask
from bq_query import bq_querier


app = Flask(__name__)

@app.route('/sales', methods=['GET'])
def bronze_to_silver_sales ():
    query = '''
        CREATE OR REPLACE TABLE `crack-battery-413519.silver.sales` PARTITION BY purchase_date AS
        SELECT CAST(CustomerId AS INTEGER) AS client_id
            , CASE
                    -- 'YYYY-MM-DD'
                WHEN REGEXP_CONTAINS(PurchaseDate, r'^\d{4}-\d{2}-\d{2}$') THEN CAST(PurchaseDate AS DATE FORMAT 'YYYY-MM-DD')
                    -- 'YYYY-MON-DD'
                WHEN REGEXP_CONTAINS(PurchaseDate, r'^\d{4}-\w{3}-\d{2}$') THEN CAST(PurchaseDate AS DATE FORMAT 'YYYY-MON-DD')
            END as purchase_date
            , Product AS product_name
            , CAST(REGEXP_REPLACE(Price, r'[^0-9.,]', '') AS INTEGER) AS price
        FROM `crack-battery-413519.bronze.sales`  ;
    '''
    res = bq_querier(query)
    
    return res, 200


@app.route('/customers', methods=['GET'])
def bronze_to_silver_customers ():
    query = '''
        CREATE OR REPLACE TABLE `crack-battery-413519.silver.customers` AS
        SELECT DISTINCT CAST(Id AS INTEGER) AS client_id
        , FirstName AS first_name
        , LastName AS last_name
        , Email AS email
        , CASE
                -- 'YYYY-MM-DD'
            WHEN REGEXP_CONTAINS(RegistrationDate, r'^\d{4}-\d{2}-\d{2}$') THEN CAST(RegistrationDate AS DATE FORMAT 'YYYY-MM-DD')
                -- 'YYYY-MON-DD'
            WHEN REGEXP_CONTAINS(RegistrationDate, r'^\d{4}-\w{3}-\d{2}$') THEN CAST(RegistrationDate AS DATE FORMAT 'YYYY-MON-DD')
            ELSE CAST(RegistrationDate AS DATE)
        END as registration_date
        , State AS state
        FROM `crack-battery-413519.bronze.customers`  ;
    '''
    res = bq_querier(query)
    
    return res, 200


@app.route('/customers_enrich', methods=['GET'])
def customers_enrich ():
    upd_cols = ['first_name', 'last_name', 'state']
    for col in upd_cols:
        query = f'''
            MERGE INTO `crack-battery-413519.silver.customers` AS trg
            USING `crack-battery-413519.silver.user_profiles` AS src
            ON trg.email = src.email
            WHEN MATCHED AND trg.{col} is null THEN
            UPDATE SET trg.{col} = SPLIT(src.full_name, ' ')[ordinal(1)]
        '''
        res = bq_querier(query)
    
    return res, 200


if __name__ == '__main__':
    app.run(port=8082)
