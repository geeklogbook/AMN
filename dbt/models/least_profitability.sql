WITH profit_data AS (
    SELECT 
        InventoryId,
        Store,
        SalesDescription,
        SalesPrice,
        PurchasePrice,
        Profit,
        ProfitMargin
    FROM {{ ref('profit_and_margin') }}
)
SELECT 
    InventoryId,
    Store,
    SalesDescription,
    SalesPrice,
    PurchasePrice,
    Profit,
    ProfitMargin
FROM 
    profit_data
ORDER BY 
    Profit ASC
LIMIT 10;