-- Catalog database for the service, plus a small demo database to back up.
CREATE DATABASE cbs;
CREATE DATABASE shop;
CREATE DATABASE shop_restored;
\connect shop
CREATE TABLE customers (id int PRIMARY KEY, name text NOT NULL, email text NOT NULL, city text NOT NULL);
INSERT INTO customers SELECT g, 'Customer ' || g, 'user' || g || '@example.com',
  (ARRAY['Bengaluru','Delhi','Mumbai','Pune','Chennai'])[1 + g % 5] FROM generate_series(1, 20000) g;
CREATE TABLE orders (id bigint PRIMARY KEY, customer_id int NOT NULL REFERENCES customers (id),
  amount_cents bigint NOT NULL, status text NOT NULL, created_at timestamptz NOT NULL, note text NOT NULL);
INSERT INTO orders SELECT g, 1 + (g::bigint * 7919) % 20000, (g::bigint * 104729) % 500000,
  (ARRAY['paid','shipped','delivered','refunded'])[1 + g % 4], timestamptz '2026-01-01' + (g || ' seconds')::interval,
  md5(g::text) FROM generate_series(1, 200000) g;
