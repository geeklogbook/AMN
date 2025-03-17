
  
    

  create  table "amn_datawarehouse"."public"."purchase_prices__dbt_tmp"
  
  
    as
  
  (
    SELECT 
    Brand,
    PurchasePrice
FROM public.purchase_prices;
  );
  