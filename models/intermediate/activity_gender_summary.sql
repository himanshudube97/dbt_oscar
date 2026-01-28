{{ config(materialized='table', schema='intermediate') }}

-- Pivot table: Count of attendees by Activity and Gender

with source as (
    select * from {{ ref('dropped_col_upshot') }}
),

aggregated as (
    select
        "Activity",
        count(distinct case when "Gender" = 'Female' then "Attendee_ID" end) as "Female",
        count(distinct case when "Gender" = 'Male' then "Attendee_ID" end) as "Male",
        count(distinct "Attendee_ID") as "Grand_Total"
    from source
    group by "Activity"
)

select * from aggregated
order by "Activity"
