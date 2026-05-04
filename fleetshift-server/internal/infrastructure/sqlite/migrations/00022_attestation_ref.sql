-- +goose Up
ALTER TABLE fulfillments ADD COLUMN attestation_ref TEXT;

-- +goose Down
ALTER TABLE fulfillments DROP COLUMN attestation_ref;
