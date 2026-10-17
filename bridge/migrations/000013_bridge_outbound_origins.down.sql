-- DESTRUCTIVE ROLLBACK ORDER:
-- 1. Stop the current bridge and restore (but do not start) the old binary.
-- 2. Apply this down migration while bridge processing remains stopped.
-- 3. Start the old binary only after the down migration succeeds.
-- Dropping the marker table and durable message classification loses all bridge
-- echo-suppression evidence. The old binary's explicit column lists tolerate the
-- additive column before this migration; the ordering is an operational safety
-- requirement, not a runtime dependency on the column being absent.
DROP TABLE IF EXISTS bridge_outbound_origins;
ALTER TABLE messages DROP COLUMN IF EXISTS is_bridge_origin;
