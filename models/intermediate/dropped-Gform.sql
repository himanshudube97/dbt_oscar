--DBT AUTOMATION has generated this model, please DO NOT EDIT 
--Please make sure you dont change the model name 

{{ config(materialized='table', schema='intermediate') }}
WITH cte1 as (
SELECT "Location", "Month_YY", "Associate", "Timestamp", "Email_Address", "Batch_Number__", "Date_of_Session__", "Type_of_Session__", "Any_other_comments__", "Life_Skill_Delivered__", "Name_of_Coach___Fellow__", "Name_of_Young_Leader_1__", "Name_of_Young_Leader_2__", "Name_of_Young_Leader_3__", "Name_of_Young_Leader_4__", "Name_of_Young_Leader_5__", "Technical_Skill_Delivered__", "Rate_the_safety_of_the_ground__", "Name_and_Description_of_Exposure__", "Changes__Cancelation_Made_In_Plan__", "Coach_s_Observation_Of_the_Group__", "Number_of_Nutrition_packets_distributed__", "Rate_the_level_of_fun_in_all_the_activities__", "Anything_special_with_the_children_or_session_", "Rate_children_s_participation_in_the_session__", "Did_children_share_their_learnings_in_the_review__", "Challenges_Faced_With_Resources__Equipment__Staff__Other__", "Photo_of_the_session___Compulsory_for_YLs_leading_Individual_se"
FROM {{ source('staging', 'gform_attendence_data') }}
)
-- Final SELECT statement combining the outputs of all CTEs
SELECT *
FROM cte1