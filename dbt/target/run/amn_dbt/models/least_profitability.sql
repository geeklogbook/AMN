
  
    

  create  table "amn_datawarehouse"."public"."least_profitability__dbt_tmp"
  
  
    as
  
  (
    WITH profit_data AS (
    SELECT 
        InventoryId,
        Store,
        SalesDescription,
        SalesPrice,
        PurchasePrice,
        Profit,
        ProfitMargin
    FROM "amn_datawarehouse"."public"."profit_and_margin"
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
LIMIT 10
  );
  