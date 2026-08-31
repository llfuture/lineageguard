-- SINK (row-preserving detail mart): one row per reported company-quarter.
-- Contrasts with the aggregate sinks: here a deleted upstream row deletes a
-- sink row (L) instead of changing an aggregate (C).
select cik, company_name, fin_year, fin_qtr,
       round(revenue, 2)  as revenue,
       round(eps, 4)      as eps,
       round(assets, 2)   as assets
from {{ ref('fact_financials') }}
