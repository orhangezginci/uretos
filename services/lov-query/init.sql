CREATE TABLE IF NOT EXISTS lov_entries (
    id VARCHAR(255) PRIMARY KEY,
    category VARCHAR(255) NOT NULL,
    code VARCHAR(255) NOT NULL,
    translations JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Optional: Ein Index für schnelles Suchen nach Kategorie
CREATE INDEX IF NOT EXISTS idx_lov_entries_category ON lov_entries(category);