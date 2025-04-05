
  
    

  create  table "amn_datawarehouse"."public"."sales__dbt_tmp"
  
  
    as
  
  (
    SELECT 
    inventoryid,
    store,
    salesdescription AS SalesDescription, 
    salesprice,
    brand
FROM public.sales
  );
  