WITH sales_data AS (
    SELECT 
        s.InventoryId,
        s.Store,
        s.SalesDescription, 
        s.SalesPrice,
        COALESCE(p.PurchasePrice, 0) AS PurchasePrice,
        (s.SalesPrice - COALESCE(p.PurchasePrice, 0)) AS Profit
    FROM 
        {{ ref('sales') }} AS s 
    LEFT JOIN 
        {{ ref('purchase_prices') }} AS p
        ON s.Brand = p.Brand
)
SELECT 
    InventoryId,
    Store,
    SalesDescription, 
    SalesPrice,
    PurchasePrice,
    Profit,
    (Profit / NULLIF(SalesPrice, 0)) * 100 AS ProfitMargin
FROM 
    sales_data;