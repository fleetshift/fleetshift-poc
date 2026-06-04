-- +goose Up
-- Orthogonal pause: pause_reason is set independently of lifecycle state.
ALTER TABLE fulfillments ADD COLUMN pause_reason TEXT NOT NULL DEFAULT '';

-- Backfill: existing paused_auth rows become active + paused.
UPDATE fulfillments
SET pause_reason = CASE WHEN status_reason != '' THEN status_reason ELSE 'delivery auth failed' END,
    state = 'active'
WHERE state = 'paused_auth';

-- +goose Down
UPDATE fulfillments SET state = 'paused_auth' WHERE pause_reason != '';
ALTER TABLE fulfillments DROP COLUMN pause_reason;
