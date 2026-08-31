select f.cik, f.fin_year, f.fin_qtr, f.revenue, f.earnings, f.eps,
       f.assets, f.liabilities, d.company_name, d.industry_name
from {{ ref('stg_financial') }} f
left join {{ ref('dim_company') }} d on f.cik = d.cik
