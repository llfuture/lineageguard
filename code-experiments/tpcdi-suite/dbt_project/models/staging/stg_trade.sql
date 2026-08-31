select
    t_id            as trade_id,
    t_dts           as trade_dts,
    t_s_symb        as symbol,
    t_ca_id         as account_id,
    t_qty           as quantity,
    t_trade_price   as trade_price,
    t_st_id         as status_id,
    t_tt_id         as trade_type_id
from {{ source('raw', 'raw_trade') }}
