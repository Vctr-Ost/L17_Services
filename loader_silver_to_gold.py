from flask import Flask
from bq_query import bq_querier


app = Flask(__name__)

@app.route('/enrich_user_profiles', methods=['GET'])
def enrich_user_profiles():
    query = '''
        CREATE OR REPLACE TABLE `crack-battery-413519.gold.user_profiles_enriched` AS
        SELECT
        c.client_id
        , c.first_name
        , c.last_name
        , c.email
        , c.registration_date
        , u.birth_date
        , c.state
        , u.phone_number
        FROM `crack-battery-413519.silver.customers` c
        LEFT JOIN `crack-battery-413519.silver.user_profiles` u
        ON c.email = u.email  ;
    '''
    res = bq_querier(query)
    
    return res, 200


if __name__ == '__main__':
    app.run(port=8083)
