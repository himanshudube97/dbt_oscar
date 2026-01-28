{{ config(materialized='table', schema='intermediate') }}

-- Unpivot Young Leader columns: each filled Young Leader becomes its own row
-- Only Name_of_Young_Leader_1__ is populated, columns 2-5 are set to NULL

with source as (
    select * from {{ ref('dropped-Gform') }}
),

unpivoted as (
    -- Young Leader 1
    select
        "Location",
        "Month_YY",
        "Associate",
        "Timestamp",
        "Email_Address",
        "Batch_Number__",
        "Date_of_Session__",
        "Type_of_Session__",
        "Any_other_comments__",
        "Life_Skill_Delivered__",
        "Name_of_Coach___Fellow__",
        "Name_of_Young_Leader_1__" as "Name_of_Young_Leader_1__",
        null as "Name_of_Young_Leader_2__",
        null as "Name_of_Young_Leader_3__",
        null as "Name_of_Young_Leader_4__",
        null as "Name_of_Young_Leader_5__",
        "Technical_Skill_Delivered__",
        "Rate_the_safety_of_the_ground__",
        "Name_and_Description_of_Exposure__",
        "Changes__Cancelation_Made_In_Plan__",
        "Coach_s_Observation_Of_the_Group__",
        "Number_of_Nutrition_packets_distributed__",
        "Rate_the_level_of_fun_in_all_the_activities__",
        "Anything_special_with_the_children_or_session_",
        "Rate_children_s_participation_in_the_session__",
        "Did_children_share_their_learnings_in_the_review__",
        "Challenges_Faced_With_Resources__Equipment__Staff__Other__",
        "Photo_of_the_session___Compulsory_for_YLs_leading_Individual_se"
    from source
    where "Name_of_Young_Leader_1__" is not null

    union all

    -- Young Leader 2
    select
        "Location",
        "Month_YY",
        "Associate",
        "Timestamp",
        "Email_Address",
        "Batch_Number__",
        "Date_of_Session__",
        "Type_of_Session__",
        "Any_other_comments__",
        "Life_Skill_Delivered__",
        "Name_of_Coach___Fellow__",
        "Name_of_Young_Leader_2__" as "Name_of_Young_Leader_1__",
        null as "Name_of_Young_Leader_2__",
        null as "Name_of_Young_Leader_3__",
        null as "Name_of_Young_Leader_4__",
        null as "Name_of_Young_Leader_5__",
        "Technical_Skill_Delivered__",
        "Rate_the_safety_of_the_ground__",
        "Name_and_Description_of_Exposure__",
        "Changes__Cancelation_Made_In_Plan__",
        "Coach_s_Observation_Of_the_Group__",
        "Number_of_Nutrition_packets_distributed__",
        "Rate_the_level_of_fun_in_all_the_activities__",
        "Anything_special_with_the_children_or_session_",
        "Rate_children_s_participation_in_the_session__",
        "Did_children_share_their_learnings_in_the_review__",
        "Challenges_Faced_With_Resources__Equipment__Staff__Other__",
        "Photo_of_the_session___Compulsory_for_YLs_leading_Individual_se"
    from source
    where "Name_of_Young_Leader_2__" is not null

    union all

    -- Young Leader 3
    select
        "Location",
        "Month_YY",
        "Associate",
        "Timestamp",
        "Email_Address",
        "Batch_Number__",
        "Date_of_Session__",
        "Type_of_Session__",
        "Any_other_comments__",
        "Life_Skill_Delivered__",
        "Name_of_Coach___Fellow__",
        "Name_of_Young_Leader_3__" as "Name_of_Young_Leader_1__",
        null as "Name_of_Young_Leader_2__",
        null as "Name_of_Young_Leader_3__",
        null as "Name_of_Young_Leader_4__",
        null as "Name_of_Young_Leader_5__",
        "Technical_Skill_Delivered__",
        "Rate_the_safety_of_the_ground__",
        "Name_and_Description_of_Exposure__",
        "Changes__Cancelation_Made_In_Plan__",
        "Coach_s_Observation_Of_the_Group__",
        "Number_of_Nutrition_packets_distributed__",
        "Rate_the_level_of_fun_in_all_the_activities__",
        "Anything_special_with_the_children_or_session_",
        "Rate_children_s_participation_in_the_session__",
        "Did_children_share_their_learnings_in_the_review__",
        "Challenges_Faced_With_Resources__Equipment__Staff__Other__",
        "Photo_of_the_session___Compulsory_for_YLs_leading_Individual_se"
    from source
    where "Name_of_Young_Leader_3__" is not null

    union all

    -- Young Leader 4
    select
        "Location",
        "Month_YY",
        "Associate",
        "Timestamp",
        "Email_Address",
        "Batch_Number__",
        "Date_of_Session__",
        "Type_of_Session__",
        "Any_other_comments__",
        "Life_Skill_Delivered__",
        "Name_of_Coach___Fellow__",
        "Name_of_Young_Leader_4__" as "Name_of_Young_Leader_1__",
        null as "Name_of_Young_Leader_2__",
        null as "Name_of_Young_Leader_3__",
        null as "Name_of_Young_Leader_4__",
        null as "Name_of_Young_Leader_5__",
        "Technical_Skill_Delivered__",
        "Rate_the_safety_of_the_ground__",
        "Name_and_Description_of_Exposure__",
        "Changes__Cancelation_Made_In_Plan__",
        "Coach_s_Observation_Of_the_Group__",
        "Number_of_Nutrition_packets_distributed__",
        "Rate_the_level_of_fun_in_all_the_activities__",
        "Anything_special_with_the_children_or_session_",
        "Rate_children_s_participation_in_the_session__",
        "Did_children_share_their_learnings_in_the_review__",
        "Challenges_Faced_With_Resources__Equipment__Staff__Other__",
        "Photo_of_the_session___Compulsory_for_YLs_leading_Individual_se"
    from source
    where "Name_of_Young_Leader_4__" is not null

    union all

    -- Young Leader 5
    select
        "Location",
        "Month_YY",
        "Associate",
        "Timestamp",
        "Email_Address",
        "Batch_Number__",
        "Date_of_Session__",
        "Type_of_Session__",
        "Any_other_comments__",
        "Life_Skill_Delivered__",
        "Name_of_Coach___Fellow__",
        "Name_of_Young_Leader_5__" as "Name_of_Young_Leader_1__",
        null as "Name_of_Young_Leader_2__",
        null as "Name_of_Young_Leader_3__",
        null as "Name_of_Young_Leader_4__",
        null as "Name_of_Young_Leader_5__",
        "Technical_Skill_Delivered__",
        "Rate_the_safety_of_the_ground__",
        "Name_and_Description_of_Exposure__",
        "Changes__Cancelation_Made_In_Plan__",
        "Coach_s_Observation_Of_the_Group__",
        "Number_of_Nutrition_packets_distributed__",
        "Rate_the_level_of_fun_in_all_the_activities__",
        "Anything_special_with_the_children_or_session_",
        "Rate_children_s_participation_in_the_session__",
        "Did_children_share_their_learnings_in_the_review__",
        "Challenges_Faced_With_Resources__Equipment__Staff__Other__",
        "Photo_of_the_session___Compulsory_for_YLs_leading_Individual_se"
    from source
    where "Name_of_Young_Leader_5__" is not null
)

select * from unpivoted
