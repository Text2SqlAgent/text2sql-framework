-- ============================================================================
-- 0008_grants.sql
--
-- Final grant pass after all gold objects exist. Default privileges from
-- 0001 cover *future* objects; this file covers *existing* ones (which is
-- everything the prior migrations created in this run).
--
-- Idempotent.
-- ============================================================================


-- ---------- gold: read-only to agent_reader and service_admin ------------

GRANT SELECT ON ALL TABLES    IN SCHEMA gold TO agent_reader, service_admin;
GRANT SELECT ON ALL SEQUENCES IN SCHEMA gold TO agent_reader, service_admin;

-- bronze + silver: read-only to service_admin only (NOT agent_reader)
GRANT SELECT ON ALL TABLES    IN SCHEMA bronze, silver TO service_admin;
GRANT SELECT ON ALL SEQUENCES IN SCHEMA bronze, silver TO service_admin;

-- etl_writer: full CRUD across all three layers
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE
  ON ALL TABLES IN SCHEMA bronze, silver, gold TO etl_writer;
GRANT USAGE, SELECT, UPDATE
  ON ALL SEQUENCES IN SCHEMA bronze, silver, gold TO etl_writer;


-- ---------- Lock down: agent_reader must NOT see bronze or silver --------
-- Belt-and-braces. The role doesn't have USAGE on those schemas, but in
-- case someone GRANTs USAGE later, we revoke any inherited table privs.

REVOKE ALL ON ALL TABLES IN SCHEMA bronze, silver FROM agent_reader;
REVOKE USAGE ON SCHEMA bronze, silver FROM agent_reader;


-- ---------- Permission to call the partition helper -----------------------

GRANT EXECUTE ON FUNCTION gold.create_monthly_partition(TEXT, TEXT, DATE)
  TO etl_writer, service_admin;


-- ---------- Sanity check ---------------------------------------------------
-- Print a summary so the operator sees this ran. Visible via psql.

DO $$
DECLARE
  agent_table_count INT;
  agent_writable_count INT;
BEGIN
  -- How many gold tables can agent_reader SELECT from?
  SELECT count(*) INTO agent_table_count
  FROM information_schema.role_table_grants
  WHERE grantee = 'agent_reader' AND privilege_type = 'SELECT'
    AND table_schema = 'gold';

  -- How many gold tables can agent_reader write to? Must be zero.
  SELECT count(*) INTO agent_writable_count
  FROM information_schema.role_table_grants
  WHERE grantee = 'agent_reader' AND privilege_type IN ('INSERT','UPDATE','DELETE','TRUNCATE')
    AND table_schema = 'gold';

  RAISE NOTICE 'agent_reader: SELECT on % gold tables, write on % (must be 0)',
    agent_table_count, agent_writable_count;

  IF agent_writable_count > 0 THEN
    RAISE EXCEPTION 'agent_reader has write privileges on gold — this must never happen.';
  END IF;
END $$;
