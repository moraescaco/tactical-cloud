CREATE TABLE IF NOT EXISTS products (
    id SERIAL PRIMARY KEY,
    sku TEXT,
    barcode TEXT,
    ncm TEXT,
    cest TEXT,
    name TEXT NOT NULL UNIQUE,
    category TEXT NOT NULL DEFAULT 'Outro',
    unit TEXT NOT NULL DEFAULT 'un',
    stock_qty DOUBLE PRECISION NOT NULL DEFAULT 0,
    min_stock DOUBLE PRECISION NOT NULL DEFAULT 0,
    cost_unit DOUBLE PRECISION NOT NULL DEFAULT 0,
    sale_price DOUBLE PRECISION NOT NULL DEFAULT 0,
    active INTEGER NOT NULL DEFAULT 1,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stock_entries (
    id SERIAL PRIMARY KEY,
    entry_date TEXT NOT NULL,
    product_id INTEGER NOT NULL REFERENCES products(id),
    qty DOUBLE PRECISION NOT NULL,
    unit_cost DOUBLE PRECISION NOT NULL,
    total_cost DOUBLE PRECISION NOT NULL,
    supplier TEXT,
    notes TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS events (
    id SERIAL PRIMARY KEY,
    event_date TEXT NOT NULL,
    name TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'Aberto',
    players INTEGER NOT NULL DEFAULT 0,
    rental_qty INTEGER NOT NULL DEFAULT 0,
    rental_unit_price DOUBLE PRECISION NOT NULL DEFAULT 0,
    own_equipment_qty INTEGER NOT NULL DEFAULT 0,
    own_equipment_unit_price DOUBLE PRECISION NOT NULL DEFAULT 0,
    entry_revenue DOUBLE PRECISION NOT NULL DEFAULT 0,
    notes TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS operators (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    residence TEXT,
    team TEXT,
    phone TEXT,
    cpf TEXT,
    active INTEGER NOT NULL DEFAULT 1,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS commands (
    id SERIAL PRIMARY KEY,
    number INTEGER NOT NULL UNIQUE,
    status TEXT NOT NULL DEFAULT 'Aberta',
    event_id INTEGER REFERENCES events(id),
    operator_id INTEGER REFERENCES operators(id),
    customer_name TEXT,
    entry_type TEXT NOT NULL DEFAULT 'Sem entrada',
    entry_value DOUBLE PRECISION NOT NULL DEFAULT 0,
    entry_original_value DOUBLE PRECISION NOT NULL DEFAULT 0,
    entry_courtesy INTEGER NOT NULL DEFAULT 0,
    entry_courtesy_reason TEXT,
    discount_percent DOUBLE PRECISION NOT NULL DEFAULT 0,
    discount_amount DOUBLE PRECISION NOT NULL DEFAULT 0,
    opened_at TEXT NOT NULL,
    closed_at TEXT,
    notes TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS sales (
    id SERIAL PRIMARY KEY,
    sale_date TEXT NOT NULL,
    product_id INTEGER NOT NULL REFERENCES products(id),
    event_id INTEGER REFERENCES events(id),
    command_id INTEGER REFERENCES commands(id),
    operator_id INTEGER REFERENCES operators(id),
    qty DOUBLE PRECISION NOT NULL,
    unit_price DOUBLE PRECISION NOT NULL,
    revenue DOUBLE PRECISION NOT NULL,
    cost_unit_at_sale DOUBLE PRECISION NOT NULL,
    cogs DOUBLE PRECISION NOT NULL,
    notes TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS expenses (
    id SERIAL PRIMARY KEY,
    expense_date TEXT NOT NULL,
    category TEXT NOT NULL,
    description TEXT NOT NULL,
    amount DOUBLE PRECISION NOT NULL,
    event_id INTEGER REFERENCES events(id),
    operator_id INTEGER REFERENCES operators(id),
    notes TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stock_movements (
    id SERIAL PRIMARY KEY,
    movement_date TEXT NOT NULL,
    product_id INTEGER NOT NULL REFERENCES products(id),
    event_id INTEGER REFERENCES events(id),
    movement_type TEXT NOT NULL,
    qty DOUBLE PRECISION NOT NULL,
    unit_cost DOUBLE PRECISION NOT NULL DEFAULT 0,
    unit_price DOUBLE PRECISION NOT NULL DEFAULT 0,
    total_cost DOUBLE PRECISION NOT NULL DEFAULT 0,
    total_revenue DOUBLE PRECISION NOT NULL DEFAULT 0,
    notes TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS system_users (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    username TEXT NOT NULL UNIQUE,
    email TEXT UNIQUE,
    password_hash TEXT NOT NULL,
    profile TEXT NOT NULL DEFAULT 'Consulta',
    active INTEGER NOT NULL DEFAULT 1,
    must_change_password INTEGER NOT NULL DEFAULT 0,
    last_login TEXT,
    visual_theme TEXT NOT NULL DEFAULT 'Tactical Couple',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS cash_sessions (
    id SERIAL PRIMARY KEY,
    status TEXT NOT NULL DEFAULT 'Aberto',
    opened_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    closed_at TEXT,
    opening_amount DOUBLE PRECISION NOT NULL DEFAULT 0,
    expected_amount DOUBLE PRECISION NOT NULL DEFAULT 0,
    closing_amount DOUBLE PRECISION NOT NULL DEFAULT 0,
    difference_amount DOUBLE PRECISION NOT NULL DEFAULT 0,
    opened_by INTEGER REFERENCES system_users(id),
    closed_by INTEGER REFERENCES system_users(id),
    notes TEXT
);

CREATE TABLE IF NOT EXISTS cash_movements (
    id SERIAL PRIMARY KEY,
    session_id INTEGER NOT NULL REFERENCES cash_sessions(id),
    movement_date TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    movement_type TEXT NOT NULL,
    description TEXT NOT NULL,
    amount DOUBLE PRECISION NOT NULL,
    payment_method TEXT NOT NULL DEFAULT 'Dinheiro',
    command_id INTEGER REFERENCES commands(id),
    created_by INTEGER REFERENCES system_users(id),
    notes TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS system_logs (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES system_users(id),
    action TEXT NOT NULL,
    details TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_products_sku_unique ON products(sku) WHERE sku IS NOT NULL AND TRIM(sku) <> '';
CREATE UNIQUE INDEX IF NOT EXISTS idx_users_email_unique ON system_users(email) WHERE email IS NOT NULL AND TRIM(email) <> '';
