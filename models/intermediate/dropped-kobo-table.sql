--DBT AUTOMATION has generated this model, please DO NOT EDIT 
--Please make sure you dont change the model name 

{{ config(materialized='table', schema='intermediate') }}
WITH cte1 as (
SELECT "_id", "end", "_tags", "_uuid", "start", "today", "_index", "_notes", "State__", "_status", "deviceid", "__version__", "Event_Type__", "Note_Remark__", "_submitted_by", "Start_location__", "_submission_time", "Date_of_Session__", "Type_of_Session__", "Goa_Batch_Number__", "_validation_status", "Goa_Young_Leader_Name__", "Name_of_Coach___Fellow_", "Karnataka_Batch_Number__", "Rajasthan_Batch_Number__", "Goa_School_Batch_Number__", "_Start_location___altitude", "_Start_location___latitude", "_Start_location___longitude", "_Start_location___precision", "Daman_and_Diu_Batch_Number__", "Karnataka_Young_Leader_Name__", "Rajasthan_Young_Leader_Name__", "DHN___DD_School_Batch_Number__", "Rajasthan_School_Batch_Number__", "Daman_and_Diu_Young_Leader_Name__", "Upload_a_photo_of_the_current_Session_Event__with_date_and_time", "Upload_a_photo_of_the_current16with_date_and_time_stamp__URL"
FROM {{ source('staging', 'kobo_raw_data') }}
)
-- Final SELECT statement combining the outputs of all CTEs
SELECT *
FROM cte1