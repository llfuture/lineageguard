select m.market_date, m.symbol, m.close_price, m.high_price, m.low_price,
       m.volume, s.cik, s.company_name, s.issue_type
from {{ ref('stg_daily_market') }} m
left join {{ ref('dim_security') }} s on m.symbol = s.symbol
