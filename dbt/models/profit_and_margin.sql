WITH sales_data AS (
    SELECT 
        s.InventoryId,
        s.Store,
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
    InventoryId,
    Store,
    SalesDescription, 
    SalesPrice,
    PurchasePrice,
    (SalesPrice - PurchasePrice) AS Profit,
    ((SalesPrice - PurchasePrice) / SalesPrice) * 100 AS ProfitMargin
FROM 
    sales_data