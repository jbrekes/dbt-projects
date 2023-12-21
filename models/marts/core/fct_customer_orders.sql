-- Import CTEs
with orders as (
    select * from {{ source('jaffle_shop', 'orders') }}
),

customers as (
    select * from {{ source('jaffle_shop', 'customers') }}
),

payments as (
    select * from {{ source('stripe', 'payment') }}    
),

-- Logical CTEs

final_payment as (
    select 
        orderid as order_id
        , max(created) as payment_finalized_date
        , sum(amount) / 100.0 as total_amount_paid
    from payments
    where status <> 'fail'
    group by 1
),

paid_orders as (
    select 
        orders.id as order_id,
        orders.user_id	as customer_id,
        orders.order_date as order_placed_at,
        orders.status as order_status,
        final_payment.total_amount_paid,
        final_payment.payment_finalized_date,
        c.first_name as customer_first_name,
        c.last_name as customer_last_name,
        sum(total_amount_paid) over(partition by orders.user_id order by order_id asc) as customer_lifetime_value,
        row_number() over (order by final_payment.order_id) as transaction_seq,
        row_number() over (partition by orders.user_id order by final_payment.order_id) as customer_sales_seq
    from orders
    left join final_payment 
        on orders.id = final_payment.order_id
    left join customers c 
        on orders.user_id = c.id 
),

customer_orders as (
select 
    c.id as customer_id
    , min(order_date) as first_order_date
    , max(order_date) as most_recent_order_date
    , count(orders.id) as number_of_orders
from customers c 
left join orders
    on orders.user_id = c.id 
group by 1
),

--  Final CTE

final as (
    select
        p.*,
        case 
            when c.first_order_date = p.order_placed_at
            then 'new'
            else 'return' 
        end as nvsr,
        c.first_order_date as fdos
    from paid_orders p
    left join customer_orders as c 
        on p.customer_id = c.customer_id
    order by order_id
)

SELECT * FROM final