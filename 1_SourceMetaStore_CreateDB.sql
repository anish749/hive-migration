-- Schemas to Migrate (This is used in like to fetch the DB names to migrate)

USE hive; -- Source MySQL MetaStore Schema

set @SCHEMA_TO_MIGRATE = '%ani%';

-- Query for creating databases
SELECT 
    CONCAT('CREATE DATABASE IF NOT EXISTS ',
            D.NAME,
            '\;') AS CREATE_DB_STATEMENTS
FROM
    DBS D
WHERE
    D.name LIKE @SCHEMA_TO_MIGRATE INTO OUTFILE 'generatedSQL/target/1_DBS.hql';
	
	
