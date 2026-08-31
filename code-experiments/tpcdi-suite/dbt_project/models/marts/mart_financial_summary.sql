-- SINK: per company-year aggregate fundamentals
select cik, company_name, fin_year,
       round(sum(revenue), 2)   as annual_revenue,
       round(avg(eps), 4)       as avg_eps,
       count(*)                 as n_quarters
from {{ ref('fact_financials') }}
group by 1, 2, 3
