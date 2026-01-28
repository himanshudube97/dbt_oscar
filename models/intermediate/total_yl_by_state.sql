--DBT AUTOMATION has generated this model, please DO NOT EDIT 
--Please make sure you dont change the model name 

{{ config(materialized='table', schema='intermediate') }}
WITH cte1 as (
SELECT
"State__",
 COUNT("_id") AS "total_yl_by_state"
FROM {{ref('dropped-kobo-table')}}
GROUP BY "State__")
-- Final SELECT statement combining the outputs of all CTEs
SELECT *
FROM cte1