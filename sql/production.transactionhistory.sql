select 
    cast(transactionid as text) as transactionid,
    cast(productid as text) as productid,
    cast(referenceorderid as text) as referenceorderid,
    cast(referenceorderlineid as text) as referenceorderlineid,
    cast(transactiondate as text) as transactiondate,
    cast(transactiontype as text) as transactiontype,
    cast(quantity as text) as quantity,
    cast(actualcost as text) as actualcost,
    cast(modifieddate as text) as modifieddate,
    cast(current_date as text) AS ingestion_date,
    'production.transactionhistory' AS data_source,
    cast(session_user as text) as current_user
from production.transactionhistory;