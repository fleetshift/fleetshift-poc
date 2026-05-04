-- +goose Up
ALTER TABLE fulfillments ADD COLUMN attestation_ref JSONB;

-- +goose Down
ALTER TABLE fulfillments DROP COLUMN attestation_ref;
