-- Trading Service — meter_read_model.is_verified (sell-side meter-verification gate)
-- ---------------------------------------------------------------------------
-- Backs the rule "a prosumer cannot open a sell order until the meter behind it
-- has been verified". Trading is the enforcement point, so it needs the fact
-- locally — it must not query the metering DB per order.
--
-- Why a new column instead of reusing `status`:
--   `meter_read_model.status` is overloaded and cannot answer this question.
--   Its DEFAULT is 'active'; the boot backfill copies metering `meters.status`
--   (an OPERATING status — active / maintenance / decommissioning); and the
--   Kafka feed writes the meter event's DERIVED 'verified'/'unverified' string.
--   Three vocabularies in one column. A gate reading it would treat a meter in
--   maintenance as unverified and — worse — a backfilled 'active' meter as
--   verified, failing OPEN on exactly the case this feature exists for.
--
-- Pre-existing rows are grandfathered to verified, deliberately:
--   Before this change meter-service set `meters.is_verified = true` at
--   registration for every meter, so every row already mirrored here represents
--   a meter that was verified under the rule in force when it was created.
--   Starting them false would revoke the sell right of every prosumer currently
--   trading — a retroactive lockout, not a security fix. The new rule binds new
--   registrations, which now start unverified and must pass
--   POST /api/v1/me/meters/{serial}/verify.
--
-- The two ALTERs express exactly that split, without an UPDATE: the ADD's
-- DEFAULT true backfills the existing rows, then the default flips to false so
-- every future insert that forgets to set the column fails CLOSED.

ALTER TABLE public.meter_read_model
    ADD COLUMN IF NOT EXISTS is_verified boolean NOT NULL DEFAULT true;

ALTER TABLE public.meter_read_model
    ALTER COLUMN is_verified SET DEFAULT false;

COMMENT ON COLUMN public.meter_read_model.is_verified IS
    'Mirror of metering meters.is_verified, fed by MeterRegistered/MeterUpdated events + boot backfill. A sell order is refused unless the seller has a verified meter. Distinct from `status`, which mirrors the meter OPERATING status.';

-- Covers the "does this seller own ANY verified meter?" lookup taken on the
-- sell path when the order names no specific meter. Partial: the gate only ever
-- asks about verified rows, so unverified meters need not be indexed.
CREATE INDEX IF NOT EXISTS idx_meter_read_model_verified_user
    ON public.meter_read_model (user_id) WHERE is_verified;
