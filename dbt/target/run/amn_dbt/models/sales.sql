
  
    

  create  table "amn_datawarehouse"."public"."sales__dbt_tmp"
  
  
    as
  
  (
    SELECT 
    InventoryId,
    Store,
    Description AS SalesDescription, 
    SalesPrice,
    Brand
FROM public.sales;
  );
  