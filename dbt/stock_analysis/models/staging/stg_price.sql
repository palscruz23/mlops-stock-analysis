SELECT 
    * 
FROM
    {{ source('stocks', 'PRICE')}}