SELECT 
    * 
FROM
    {{ source('stocks', 'PRICE')}}
WHERE
    TICKER = '{{ var("ticker") }}'