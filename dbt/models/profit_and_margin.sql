SELECT 
    s.InventoryId,
    s.Store,
    s.SalesDescription, 
    s.SalesPrice,
    COALESCE(p.PurchasePrice, 0) AS PurchasePrice,
    (s.SalesPrice - COALESCE(p.PurchasePrice, 0)) AS Profit,
    (s.SalesPrice - COALESCE(p.PurchasePrice, 0)) / NULLIF(s.SalesPrice, 0) * 100 AS ProfitMargin
FROM 
    {{ ref('sales') }} AS s 
LEFT JOIN 
    {{ ref('purchase_prices') }} AS p
    ON s.Brand = p.Brand