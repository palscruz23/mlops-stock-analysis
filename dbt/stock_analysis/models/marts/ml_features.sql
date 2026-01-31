{{ config(materialized='table') }}

WITH base as (SELECT
    *,
    LEAD(CLOSE, 1) OVER (PARTITION BY TICKER ORDER BY DATETIME) AS NEXT_PRICE,
FROM   
    {{ ref('int_price_features') }}
    )

SELECT
    *,
    CASE WHEN NEXT_PRICE > CLOSE THEN 1 ELSE 0 END AS LABEL
FROM   
    base
WHERE NEXT_PRICE IS NOT NULL