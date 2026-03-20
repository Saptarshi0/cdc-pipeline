-- ═══════════════════════════════════════════════════════════════════════════
-- CDC Pipeline - PostgreSQL Initialization
-- ═══════════════════════════════════════════════════════════════════════════

-- ── Replication role for Debezium ─────────────────────────────────────────
CREATE ROLE replicator REPLICATION LOGIN PASSWORD 'replicator_secret';

-- Grant SELECT on all existing tables
GRANT CONNECT ON DATABASE cdc_db TO replicator;
GRANT USAGE   ON SCHEMA public    TO replicator;
GRANT SELECT  ON ALL TABLES IN SCHEMA public TO replicator;

-- Grant SELECT on future tables automatically
ALTER DEFAULT PRIVILEGES IN SCHEMA public
  GRANT SELECT ON TABLES TO replicator;

-- ── Publication (WAL-level change capture) ────────────────────────────────
-- Publishes INSERT, UPDATE, DELETE for all tables.
-- Add specific tables: FOR TABLE orders, customers, products
CREATE PUBLICATION cdc_publication FOR ALL TABLES;

-- ── Sample domain tables ─────────────────────────────────────────────────

CREATE TABLE customers (
    id          BIGSERIAL PRIMARY KEY,
    email       TEXT        NOT NULL UNIQUE,
    full_name   TEXT        NOT NULL,
    phone       TEXT,
    status      TEXT        NOT NULL DEFAULT 'active'
                            CHECK (status IN ('active','suspended','deleted')),
    metadata    JSONB,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE products (
    id          BIGSERIAL PRIMARY KEY,
    sku         TEXT        NOT NULL UNIQUE,
    name        TEXT        NOT NULL,
    price       NUMERIC(12,2) NOT NULL,
    stock_qty   INT         NOT NULL DEFAULT 0,
    category    TEXT,
    active      BOOLEAN     NOT NULL DEFAULT true,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE orders (
    id          BIGSERIAL PRIMARY KEY,
    customer_id BIGINT      NOT NULL REFERENCES customers(id),
    status      TEXT        NOT NULL DEFAULT 'pending'
                            CHECK (status IN ('pending','confirmed','shipped','delivered','cancelled')),
    total_amount NUMERIC(12,2) NOT NULL,
    currency    TEXT        NOT NULL DEFAULT 'USD',
    metadata    JSONB,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE order_items (
    id          BIGSERIAL PRIMARY KEY,
    order_id    BIGINT      NOT NULL REFERENCES orders(id),
    product_id  BIGINT      NOT NULL REFERENCES products(id),
    quantity    INT         NOT NULL,
    unit_price  NUMERIC(12,2) NOT NULL
);

-- REPLICA IDENTITY FULL ensures UPDATE and DELETE events carry the full old row.
-- Default is PRIMARY KEY only. Set FULL for tables without PK or for audit needs.
ALTER TABLE customers   REPLICA IDENTITY FULL;
ALTER TABLE products    REPLICA IDENTITY FULL;
ALTER TABLE orders      REPLICA IDENTITY FULL;
ALTER TABLE order_items REPLICA IDENTITY FULL;

-- ── Indexes ───────────────────────────────────────────────────────────────
CREATE INDEX idx_orders_customer_id  ON orders(customer_id);
CREATE INDEX idx_orders_status       ON orders(status);
CREATE INDEX idx_orders_created_at   ON orders(created_at DESC);
CREATE INDEX idx_order_items_order   ON order_items(order_id);
CREATE INDEX idx_customers_email     ON customers(email);

-- ── updated_at trigger ────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$;

CREATE TRIGGER trg_customers_updated_at
  BEFORE UPDATE ON customers
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE TRIGGER trg_products_updated_at
  BEFORE UPDATE ON products
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE TRIGGER trg_orders_updated_at
  BEFORE UPDATE ON orders
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- ── Seed data ─────────────────────────────────────────────────────────────
INSERT INTO customers (email, full_name, phone, status)
VALUES
  ('alice@example.com',   'Alice Kumar',   '+91-9800000001', 'active'),
  ('bob@example.com',     'Bob Smith',     '+91-9800000002', 'active'),
  ('charlie@example.com', 'Charlie Wang',  '+91-9800000003', 'active');

INSERT INTO products (sku, name, price, stock_qty, category)
VALUES
  ('SKU-001', 'Wireless Keyboard', 49.99,  200, 'Electronics'),
  ('SKU-002', 'USB-C Hub',         29.99,  500, 'Electronics'),
  ('SKU-003', 'Laptop Stand',      39.99,  150, 'Accessories');

INSERT INTO orders (customer_id, status, total_amount)
VALUES
  (1, 'confirmed', 49.99),
  (2, 'pending',   79.98);

INSERT INTO order_items (order_id, product_id, quantity, unit_price)
VALUES
  (1, 1, 1, 49.99),
  (2, 2, 1, 29.99),
  (2, 3, 1, 39.99);
