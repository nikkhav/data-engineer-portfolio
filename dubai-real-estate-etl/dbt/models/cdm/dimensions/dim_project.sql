{{ config(materialized='table') }}

WITH projects AS (
    SELECT DISTINCT
        project        as project_name,
        area           as area_name,
        master_project,
        nearest_mall,
        nearest_metro,
        nearest_landmark
    FROM {{ ref('sat_property_details') }}
    WHERE project IS NOT NULL
)


SELECT
    {{ dbt_utils.generate_surrogate_key(['project_name']) }} AS project_key,
    project_name,
    area_name,
    master_project,
    nearest_mall,
    nearest_metro,
    nearest_landmark
FROM projects