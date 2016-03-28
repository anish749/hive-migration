-- Query for Creating Show CT Statements to be run in the Source Hive Installation

set @SCHEMA_TO_MIGRATE = '%ani%';

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
        AND D.name LIKE @SCHEMA_TO_MIGRATE INTO OUTFILE 'generatedSQL_source/2_CT.hql';
		
