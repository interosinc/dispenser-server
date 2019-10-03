BEGIN;

CREATE FUNCTION drop_test_partitions()
RETURNS VOID AS $$

DECLARE
  tables CURSOR FOR
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema='public'
    AND table_name LIKE 'test_partition_%';
  t TEXT;
BEGIN
  FOR t IN tables LOOP
    EXECUTE format('DROP TABLE %1$I'
           , TRIM(LEADING '('
                  FROM ( TRIM(TRAILING ')'
                              FROM (t :: TEXT)))));
  END LOOP;
END;

$$ LANGUAGE 'plpgsql';

SELECT drop_test_partitions();

DROP FUNCTION drop_test_partitions();

COMMIT;
