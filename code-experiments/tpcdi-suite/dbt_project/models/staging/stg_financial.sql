-- FINWIRE FIN records. Conflict site: numeric corruption on revenue vs
-- duplicated (cik, year, quarter) rows.
select
    trim(substr(line, 187, 60))                     as cik,
    try_cast(substr(line, 19, 4) as integer)        as fin_year,
    try_cast(substr(line, 23, 1) as integer)        as fin_qtr,
    try_cast(trim(substr(line, 40, 17)) as double)  as revenue,
    try_cast(trim(substr(line, 57, 17)) as double)  as earnings,
    try_cast(trim(substr(line, 74, 12)) as double)  as eps,
    try_cast(trim(substr(line, 127, 17)) as double) as assets,
    try_cast(trim(substr(line, 144, 17)) as double) as liabilities
from {{ source('raw', 'raw_finwire') }}
where rec_type = 'FIN'
