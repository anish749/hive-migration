-- Query to create add partition scripts to run in target Hive Installation

SET @SCHEMA_TO_MIGRATE = '%ani%';
SET @oldNameNode = 'hdfs://localhost:9000/'; -- Old NameNode address
SET @newNameNode = 'hdfs://newhostName:8020/'; -- New NameNode address

SELECT 
    REPLACE(add_partition_query,
        @oldNameNode,
        @newNameNode) AS add_partition_query
	
FROM			
	(SELECT
		CONCAT('USE ', D.name, '\; ', ' ALTER TABLE ', T.TBL_NAME, ' ADD PARTITION (', GROUP_CONCAT(PK.PKEY_NAME, '=', '\'', PKV.PART_KEY_VAL, '\''
                    ORDER BY PK.INTEGER_IDX), ') LOCATION \'', S.location, '\'\; ') AS add_partition_query
    FROM
        TBLS T
    INNER JOIN DBS D ON T.DB_ID = D.DB_ID
    INNER JOIN PARTITION_KEYS PK ON T.TBL_ID = PK.TBL_ID
    INNER JOIN PARTITIONS P ON P.TBL_ID = T.TBL_ID
    INNER JOIN PARTITION_KEY_VALS PKV ON P.PART_ID = PKV.PART_ID
        AND PK.INTEGER_IDX = PKV.INTEGER_IDX
    INNER JOIN SDS S ON P.SD_ID = S.SD_ID
    WHERE
        D.name LIKE @SCHEMA_TO_MIGRATE
    GROUP BY P.PART_ID
    ORDER BY D.name) alias1 INTO OUTFILE 'generatedSQL_target/3_ADD_PARTITION.hql';
    
    
