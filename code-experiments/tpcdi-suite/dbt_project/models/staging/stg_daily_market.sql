-- Conflict site: numeric corruption on close price vs duplicated (date, symbol).
select
    dm_date                     as market_date,
    dm_s_symb                   as symbol,
    dm_close                    as close_price,
    dm_high                     as high_price,
    dm_low                      as low_price,
    dm_vol                      as volume
from {{ source('raw', 'raw_dailymarket') }}
