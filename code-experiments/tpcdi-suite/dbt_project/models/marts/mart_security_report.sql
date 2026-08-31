-- SINK: per security market aggregates
select symbol, company_name, issue_type,
       round(avg(close_price), 4) as avg_close,
       round(max(high_price), 4)  as max_high,
       sum(volume)                as total_volume,
       count(*)                   as n_days
from {{ ref('fact_market_history') }}
group by 1, 2, 3
