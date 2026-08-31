-- SINK: per customer trading activity
select customer_id,
       count(*)                        as n_trades,
       round(sum(trade_value), 2)      as total_trade_value,
       round(avg(trade_price), 4)      as avg_trade_price,
       count(distinct symbol)          as n_symbols
from {{ ref('fact_trade') }}
where customer_id is not null
group by 1
