select t.trade_id, t.trade_dts, t.symbol, t.account_id, t.quantity,
       t.trade_price, t.quantity * t.trade_price as trade_value,
       a.customer_id, s.cik, s.company_name
from {{ ref('stg_trade') }} t
left join {{ ref('stg_account') }} a on t.account_id = a.account_id
left join {{ ref('dim_security') }} s on t.symbol = s.symbol
