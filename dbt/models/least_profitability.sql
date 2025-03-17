WITH sales_data AS (
    SELECT 
        s.Brand,
        s.SalesDescription, 
        s.SalesPrice,
        p.PurchasePrice
    FROM 
        {{ ref('Sales') }} s
    LEFT JOIN 
        {{ ref('PurchasePrices') }} p
        ON s.Brand = p.Brand
)
SELECT 
    Brand,
    SalesDescription, 
    (SalesPrice - PurchasePrice) AS Profit  
FROM 
    sales_data
WHERE 
    (SalesPrice - PurchasePrice) < 0