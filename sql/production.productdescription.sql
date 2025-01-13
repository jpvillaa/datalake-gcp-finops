SELECT 
    cast(productdescriptionid as text) as productdescriptionid,
    cast(description as text) as description,
    cast(rowguid as text) as rowguid,
    cast(modifieddate as text) as modifieddate,
    cast(current_date as text) AS ingestion_date,
    'production.productdescription' AS data_source,
    cast(session_user as text) AS current_user
FROM production.productdescription;