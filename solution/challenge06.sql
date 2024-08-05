-- create a network rule for the api that i will be accessing 
CREATE OR REPLACE NETWORK RULE api_network_rule
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = ('api.open-meteo.com');

-- create an external access integration to enable access to the external network location 
CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION apis_access_integration
  ALLOWED_NETWORK_RULES = (api_network_rule)
  ENABLED = true;

-- Python stored procedure that 
--      - uses the external access integration to access the weather api 
--      - reads hourly weather forecast data (for the next 14 days) from the api
--      - creates an aggregation in python 
--      - writes data to snowflake table 
CREATE OR REPLACE PROCEDURE accessExternalApi(timezone STRING)
RETURNS STRING
LANGUAGE python
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python', 'requests', 'pandas', 'numpy')
EXTERNAL_ACCESS_INTEGRATIONS = (apis_access_integration)
HANDLER = 'access_external_api' 
AS
$$
import requests
from datetime import datetime, date
import numpy as np
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

def access_external_api(session, timezone STRING): 
    # params for the api request 
    params = {
	"latitude": 40.75,
	"longitude": -73.99,
	"hourly": ["temperature_2m"],
    "temperature_unit": "fahrenheit",
    "timezone": timezone,
    "forecast_days": 14
    }
    # this will return the hourly weather forecast for the next 14 days for NYC, in the requested timezone 
    data = requests.get("https://api.open-meteo.com/v1/forecast", params=params).json()

    # grabs the temperature from the json 
    temp = (data['hourly']['temperature_2m'])
    # grabs the datetime from the json, converting the strings to datetime format 
    time = [datetime.strptime(x, "%Y-%m-%dT%H:%M") for x in (data['hourly']['time'])]
    # creates a dict with the datetime, date, chosen timezone, and temperature 
    hourly_weather = {"datetime":time, "date":[(x.date()) for x in time], "timezone": params["timezone"], "temp":temp}

    # creates a pandas df from the weather data dict 
    hourly_weather_df = pd.DataFrame(data = hourly_weather)
    # groups df by date and timezone 
    weather_groupedbyday_df = hourly_weather_df.groupby(["date", "timezone"])
    # applies min & max aggregation to get the min and max temp for each day and timezone 
    weather_aggr_df = weather_groupedbyday_df.agg(minimum_temp=('temp', np.min), 
        maximum_temp=('temp', np.max))
    # writes result to table (creating table if it doesn't exit and replacing the data)
    table = "WeatherForecast_14Day"
    session.write_pandas(weather_aggr_df, table, auto_create_table=True,overwrite=True)    
$$;

-- calls the stored procedure 
call accessExternalApi("America/New_York");

-- creates a dynamic table that queries the WeatherForecast_14Day table for 
-- max/min temp in the next week (current date included) where the high will be over 85 degrees 
CREATE OR REPLACE DYNAMIC TABLE forecasted_hot_days 
 TARGET_LAG = '1 MINUTE'
  WAREHOUSE = SYSTEM$STREAMLIT_NOTEBOOK_WH
  AS
    SELECT date, timezone, minimum_temp, maximum_temp
    FROM WeatherForecast_14Day 
    WHERE date <= CURRENT_DATE() + INTERVAL '6 DAY'
    AND maximum_temp > 85
    ORDER BY date;

-- creates a role mapping table to use in the row access policy 
-- has the timezone and role name to be assigned to users 
CREATE OR REPLACE TABLE test.public.role_mapping(
    timezone varchar(50),
    role_name varchar(50)
);

-- adds roles to the role_mapping table 
INSERT INTO test.public.role_mapping(timezone, role_name) VALUES
('America/New_York','VIEWER_EST'),
('America/Denver','VIEWER_MDT'),
('America/Chicago','VIEWER_CST')
;

-- creates a row access policy where users with 'SYSADMIN' can query all rows in the table 
-- or users can query rows belonging to their timezone, using the values from the role_mapping table 
CREATE OR REPLACE ROW ACCESS POLICY rap AS (timezone varchar) RETURNS boolean ->
    'SYSADMIN' = current_role() 
        or exists (
            select 1 from role_mapping r
              where role_name = current_role()
                and r.timezone = timezone
        )
;

-- adds the row access policy to the dynamic forecasted_hot_days table 
ALTER TABLE forecasted_hot_days 
ADD ROW ACCESS POLICY rap ON (timezone);

-- creates custom roles from the role_mapping table 
USE ROLE ACCOUNTADMIN;

CREATE IF NOT EXISTS ROLE VIEWER_EST;
CREATE IF NOT EXISTS ROLE VIEWER_MDT;
CREATE IF NOT EXISTS ROLE VIEWER_CST;

-- assigns custom roles to sysadmin so sysadmin inherits their permissions 
GRANT ROLE VIEWER_EST TO ROLE SYSADMIN;
GRANT ROLE VIEWER_MDT TO ROLE SYSADMIN;
GRANT ROL VIEWER_CST TO ROLE SYSADMIN;

-- grants privileges to the roles 
GRANT USAGE ON database test to role VIEWER_EST;
GRANT USAGE ON schema public to role VIEWER_EST;
GRANT SELECT ON all tables in schema test to role VIEWER_EST;

GRANT USAGE ON database test to role VIEWER_MDT;
GRANT USAGE ON schema public to role VIEWER_MDT;
GRANT SELECT ON all tables in schema test to role VIEWER_MDT;

GRANT USAGE ON database test to role VIEWER_CST;
GRANT USAGE ON schema public to role VIEWER_CST;
GRANT SELECT ON all tables in schema test to role VIEWER_CST;

-- assigns the custom roles to users 
GRANT ROLE VIEWER_EST TO USER jackie_est;
GRANT ROLE VIEWER_MDT TO USER jackie_mdt;

-- * FOR TESTING ROLE ACCESS - should return est rows * 
-- USE ROLE jackie_est;
-- USE DATABASE test; 
-- USE SCHEMA public; 
-- select * from forecasted_hot_days; 

-- * FOR TESTING ROLE ACCESS - should not return anything if the table only has est data* 
-- USE ROLE jackie_mdt;
-- USE DATABASE test; 
-- USE SCHEMA public; 
-- select * from forecasted_hot_days; 





