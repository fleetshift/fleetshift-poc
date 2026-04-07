-- +goose Up
-- Hard cutover: drop the old signing_key_bindings table and create
-- the new signer_enrollments table. Existing key bindings are
-- invalidated — users must re-enroll.
DROP INDEX IF EXISTS idx_skb_subject;
DROP TABLE IF EXISTS signing_key_bindings;

CREATE TABLE signer_enrollments (
    id TEXT PRIMARY KEY,
    subject_id TEXT NOT NULL,
    issuer TEXT NOT NULL,
    identity_token TEXT NOT NULL,
    registry_subject TEXT NOT NULL,
    registry_id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    expires_at TEXT NOT NULL
);
CREATE INDEX idx_se_subject ON signer_enrollments(subject_id, issuer);

-- +goose Down
DROP INDEX IF EXISTS idx_se_subject;
DROP TABLE IF EXISTS signer_enrollments;

CREATE TABLE signing_key_bindings (
    id TEXT PRIMARY KEY,
    subject_id TEXT NOT NULL,
    issuer TEXT NOT NULL,
    public_key_jwk BLOB NOT NULL,
    algorithm TEXT NOT NULL,
    key_binding_doc BLOB NOT NULL,
    key_binding_signature BLOB NOT NULL,
    identity_token TEXT NOT NULL,
    created_at TEXT NOT NULL,
    expires_at TEXT NOT NULL
);
CREATE INDEX idx_skb_subject ON signing_key_bindings(subject_id, issuer);
