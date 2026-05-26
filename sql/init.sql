CREATE TABLE IF NOT EXISTS products (
    id INT PRIMARY KEY,
    name TEXT NOT NULL,
    stock INT NOT NULL,
    price INT NOT NULL
);

ALTER TABLE products REPLICA IDENTITY FULL;

CREATE PUBLICATION dbz_pub FOR TABLE public.products;