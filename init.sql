DO
$$
BEGIN
   IF NOT EXISTS (SELECT FROM pg_database WHERE datname = 'amn_datawarehouse') THEN
      CREATE DATABASE amn_datawarehouse;
   END IF;
END
$$;

\c amn_datawarehouse;

-- Crear tabla purchase_prices
CREATE TABLE IF NOT EXISTS public.purchase_prices (
    id SERIAL PRIMARY KEY,
    Brand TEXT NOT NULL,
    PurchasePrice NUMERIC NOT NULL
);

-- Crear tabla sales
CREATE TABLE IF NOT EXISTS public.sales (
    id SERIAL PRIMARY KEY,
    InventoryId INT NOT NULL,
    Store TEXT NOT NULL,
    salesdescription TEXT NOT NULL,
    SalesPrice NUMERIC NOT NULL,
    Brand TEXT NOT NULL
);