-- +goose Up
CREATE TABLE IF NOT EXISTS inventory_edges (
    target_id    TEXT NOT NULL,
    source_uid   TEXT NOT NULL,
    dest_uid     TEXT NOT NULL,
    edge_type    TEXT NOT NULL,
    source_kind  TEXT NOT NULL,
    dest_kind    TEXT NOT NULL,
    PRIMARY KEY (target_id, source_uid, dest_uid, edge_type)
);

CREATE INDEX IF NOT EXISTS idx_inventory_edges_dest ON inventory_edges (target_id, dest_uid);

-- +goose Down
DROP INDEX IF EXISTS idx_inventory_edges_dest;
DROP TABLE IF EXISTS inventory_edges;
