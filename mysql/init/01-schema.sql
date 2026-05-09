-- Grant replication privileges to the Debezium user
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'debezium'@'%';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'debezium'@'%';
FLUSH PRIVILEGES;

USE ecommerce;

-- Customers
CREATE TABLE IF NOT EXISTS customers (
    id           BIGINT AUTO_INCREMENT PRIMARY KEY,
    email        VARCHAR(255) NOT NULL UNIQUE,
    first_name   VARCHAR(100) NOT NULL,
    last_name    VARCHAR(100) NOT NULL,
    city         VARCHAR(100) NOT NULL,
    country      VARCHAR(100) NOT NULL DEFAULT 'AE',
    tier         ENUM('STANDARD','SILVER','GOLD','PLATINUM') NOT NULL DEFAULT 'STANDARD',
    created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- Products
CREATE TABLE IF NOT EXISTS products (
    id              BIGINT AUTO_INCREMENT PRIMARY KEY,
    sku             VARCHAR(50)  NOT NULL UNIQUE,
    name            VARCHAR(255) NOT NULL,
    category        ENUM('ELECTRONICS','GROCERY','FASHION','HOME','TRAVEL','OTHER') NOT NULL,
    price           DECIMAL(10,2) NOT NULL,
    stock           INT NOT NULL DEFAULT 100,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- Orders
CREATE TABLE IF NOT EXISTS orders (
    id              BIGINT AUTO_INCREMENT PRIMARY KEY,
    order_ref       VARCHAR(36) NOT NULL UNIQUE,
    customer_id     BIGINT NOT NULL,
    product_id      BIGINT NOT NULL,
    quantity        INT NOT NULL DEFAULT 1,
    unit_price      DECIMAL(10,2) NOT NULL,
    total_amount    DECIMAL(12,2) GENERATED ALWAYS AS (quantity * unit_price) STORED,
    currency        VARCHAR(3) NOT NULL DEFAULT 'AED',
    status          ENUM('PLACED','CONFIRMED','SHIPPED','DELIVERED','CANCELLED') NOT NULL DEFAULT 'PLACED',
    channel         ENUM('WEB','MOBILE','STORE') NOT NULL DEFAULT 'WEB',
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (customer_id) REFERENCES customers(id),
    FOREIGN KEY (product_id)  REFERENCES products(id)
);

-- Seed reference data
INSERT INTO customers (email, first_name, last_name, city, country, tier) VALUES
  ('ahmed.al-rashid@example.com','Ahmed','Al-Rashid','Dubai','AE','GOLD'),
  ('sara.johnson@example.com','Sara','Johnson','Abu Dhabi','AE','SILVER'),
  ('raj.patel@example.com','Raj','Patel','Dubai','AE','PLATINUM'),
  ('maria.garcia@example.com','Maria','Garcia','Sharjah','AE','STANDARD'),
  ('james.li@example.com','James','Li','Dubai','AE','GOLD'),
  ('fatima.al-zaabi@example.com','Fatima','Al-Zaabi','Al Ain','AE','SILVER'),
  ('lucas.muller@example.com','Lucas','Müller','Dubai','AE','STANDARD'),
  ('priya.sharma@example.com','Priya','Sharma','Abu Dhabi','AE','GOLD');

INSERT INTO products (sku, name, category, price) VALUES
  ('ELEC-001','Sony WH-1000XM5 Headphones','ELECTRONICS', 1499.00),
  ('ELEC-002','Apple iPad Pro 11"','ELECTRONICS', 3799.00),
  ('ELEC-003','Samsung Galaxy S24','ELECTRONICS', 3299.00),
  ('GROC-001','Organic Date Box 1kg','GROCERY', 89.00),
  ('GROC-002','Premium Saffron 2g','GROCERY', 149.00),
  ('FASH-001','Linen Kandura','FASHION', 299.00),
  ('FASH-002','Designer Abaya','FASHION', 599.00),
  ('HOME-001','Nespresso Vertuo Coffee Machine','HOME', 849.00),
  ('TRVL-001','Emirates Business Class Upgrade','TRAVEL', 4500.00),
  ('ELEC-004','Dell XPS 15 Laptop','ELECTRONICS', 7299.00);
