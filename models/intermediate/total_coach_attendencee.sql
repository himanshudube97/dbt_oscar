--DBT AUTOMATION has generated this model, please DO NOT EDIT 
--Please make sure you dont change the model name 

{{ config(materialized='table', schema='intermediate') }}
WITH cte1 as (
SELECT
"Name_of_Coach___Fellow_",
 COUNT("_id") AS "total_attendence_of_coach"
FROM {{ref('dropped-kobo-table')}}
GROUP BY "Name_of_Coach___Fellow_")
-- Final SELECT statement combining the outputs of all CTEs
SELECT *
FROM cte1