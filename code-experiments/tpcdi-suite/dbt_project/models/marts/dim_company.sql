select c.cik, c.company_name, c.status, c.industry_id, c.sp_rating,
       i.in_name as industry_name
from {{ ref('stg_company') }} c
left join {{ source('raw', 'raw_industry') }} i on c.industry_id = i.in_id
