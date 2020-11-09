with customers as (
    select * from {{ ref('stg_customers') }}
),
orders as (
    select * from {{ ref('stg_orders') }}
),
payments as (
    select * from {{ ref('stg_payments') }}
),
customer_orders as (
    select
        ord.customer_id
        ,min(ord.order_date) as first_order_date
        ,max(ord.order_date) as most_recent_order_date
        ,count(ord.order_id) as number_of_orders
        ,sum(pmt.amount) as payment_amount
    from orders ord
    left join payments pmt
        on ord.order_id = pmt.order_id
        and pmt.status = 'success'
    group by 1
),
final as (
    select
        customers.customer_id,
        customers.first_name,
        customers.last_name,
        customer_orders.first_order_date,
        customer_orders.most_recent_order_date,
        coalesce(customer_orders.number_of_orders, 0) as number_of_orders,
        coalesce(customer_orders.payment_amount,0) as lifetime_value
    from customers
    left join customer_orders using (customer_id)
)
select * from final