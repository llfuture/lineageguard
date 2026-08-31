-- SINK: per trading day market aggregates
select market_date,
       round(avg(close_price), 4) as avg_close,
       sum(volume)                as total_volume,
       count(distinct symbol)     as n_symbols
from {{ ref('fact_market_history') }}
group by 1
