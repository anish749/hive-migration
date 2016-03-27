-- Query for Creating Show CT Statements to be run in the Source Hive Installation

SELECT 
    CONCAT('SHOW CREATE TABLE ',
            D.name,
            '.',
            T.tbl_name,
            '\;') AS SHOW_CT_STATEMENTS
FROM
    TBLS T
        INNER JOIN
    DBS D ON D.DB_ID = T.DB_ID
WHERE
    T.TBL_TYPE != 'INDEX_TABLE'
        AND D.name LIKE @SCHEMA_TO_MIGRATE INTO OUTFILE 'generatedSQL/target/2_CT.hql';
		
