-- Ledger schema and seed (spec §6.2); runs once when the container is created.
CREATE TABLE accounts (id integer PRIMARY KEY, balance_cents bigint NOT NULL);
CREATE TABLE entries (
  payment_id   text PRIMARY KEY,
  account_id   integer NOT NULL,
  amount_cents bigint NOT NULL,
  auth_code    text NOT NULL,
  created_at   timestamptz NOT NULL DEFAULT now()
);
INSERT INTO accounts SELECT g, 1000000000000000 FROM generate_series(1, 10000) g;
