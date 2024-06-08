BEGIN;

-- DROP TABLE IF EXISTS public."Bill";
-- CREATE TABLE public."Bill"
-- (
--     customerid integer NOT NULL,
--     category "char" NOT NULL,
--     country "char" NOT NULL,
--     industry "char" NOT NULL,
--     month "char" NOT NULL,
--     billedamount FLOAT NOT NULL
-- );

-- COPY public."Bill" FROM '/Users/nipphittt/Billing_data.csv' WITH (FORMAT csv, HEADER);

-- INSERT INTO public."Bill"(customerid,category,country,industry,month,billedamount) 
-- VALUES(1,'Individual','Indonesia','Engineering','2009-1',5060);


SELECT * FROM public."Bill";
END;