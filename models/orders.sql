with orders as (
    select * from {{ ref('stg_orders') }}
),
payments as (
    select * from {{ ref('stg_payments') }}
)
SELECT
    ORD.ORDER_ID
    ,ORD.CUSTOMER_ID
    ,SUM(PMT.AMOUNT) AS TOTAL_PAYMENT_AMOUNT
    ,SUM(CASE WHEN PMT.STATUS = 'fail' THEN 0 ELSE PMT.AMOUNT END) AS TOTAL_SUCCESS_AMOUNT
    ,SUM(CASE WHEN PMT.STATUS = 'fail' THEN PMT.AMOUNT ELSE 0 END) AS TOTAL_FAIL_AMOUNT
FROM orders ORD
LEFT OUTER JOIN payments PMT
    ON ORD.ORDER_ID = PMT.ORDER_ID
GROUP BY 
    ORD.ORDER_ID
    ,ORD.CUSTOMER_ID