source config.properties

#Hive Script Path for source Hive Installation Scripts
outputSourcePath=$(pwd)/generatedSQL_source_Hive

#Hive Script Path for target Hive Installation Scripts
outputTargetPath=$(pwd)/generatedSQL_target_Hive

mysql -h$MYSQL_SERVER_HOST -u $HIVE_METASTORE_USER -p$HIVE_METASTORE_PASS -D$HIVE_METASTORE_SCHEMA --skip-column-names -e"
-- Query to Create COUNT(*) Validation queries for Non Partitioned tables.

SET @SCHEMA_TO_MIGRATE = '$SCHEMA_TO_MIGRATE';

SELECT CONCAT (
        'SELECT \'', D.NAME, '\', \'', T.TBL_NAME, '\', COUNT(*) FROM '
        ,D.NAME
        ,'\.'
        ,T.TBL_NAME
        ,'\;'
        ) AS COUNT_TBL_QUERY
FROM DBS D
INNER JOIN TBLS T ON T.DB_ID = D.DB_ID
WHERE D.NAME LIKE @SCHEMA_TO_MIGRATE
    AND NOT EXISTS (
        SELECT 1
        FROM PARTITIONS P
        WHERE P.TBL_ID = T.TBL_ID
        ); " >$outputSourcePath/2_COUNT_NON_PARTITION_TBLS.hql
ret=$?
if [ $ret -ne 0 ];
then
	echo "Error - MySQL Error code is $ret while trying to extract COUNT validation scripts for non partitioned tables"
	exit $ret
else
	echo "COUNT validation scripts for non partitioned tables completed successfully. Stored in $outputSourcePath/2_COUNT_NON_PARTITION_TBLS.hql"
fi   
	
	
mysql -h$MYSQL_SERVER_HOST -u $HIVE_METASTORE_USER -p$HIVE_METASTORE_PASS -D$HIVE_METASTORE_SCHEMA --skip-column-names -e"	
-- Query to Create COUNT(*) Validation queries at a partition level for Partitioned tables.	

SET @SCHEMA_TO_MIGRATE = '$SCHEMA_TO_MIGRATE';

SELECT CONCAT (
        'SELECT \'', D.NAME, '\', \'', T.TBL_NAME, '\', '
        ,GROUP_CONCAT(PK.PKEY_NAME ORDER BY PK.INTEGER_IDX)
        ,', COUNT(*) FROM '
        ,D.NAME
        ,'\.'
        ,T.TBL_NAME
        ,' GROUP BY '
        ,GROUP_CONCAT(PK.PKEY_NAME ORDER BY PK.INTEGER_IDX)
        ,' ORDER BY '
        ,GROUP_CONCAT(PK.PKEY_NAME ORDER BY PK.INTEGER_IDX)
        ,'\;'
        )
FROM TBLS T
INNER JOIN DBS D ON T.DB_ID = D.DB_ID
INNER JOIN PARTITION_KEYS PK ON T.TBL_ID = PK.TBL_ID
INNER JOIN PARTITIONS P ON P.TBL_ID = T.TBL_ID
WHERE D.NAME LIKE @SCHEMA_TO_MIGRATE
GROUP BY P.PART_ID
ORDER BY D.NAME;  " >$outputSourcePath/3_COUNT_PARTITIONS_TBLS.hql
ret=$?
if [ $ret -ne 0 ];
then
	echo "Error - MySQL Error code is $ret while trying to extract COUNT validation scripts for partitioned tables"
	exit $ret
else
	echo "COUNT validation scripts for partitioned tables completed successfully. Stored in $outputSourcePath/3_COUNT_PARTITIONS_TBLS.hql"
fi


# Rename schema names
sed "s/$oldschemaname/$newschemaname/g" $outputSourcePath/2_COUNT_NON_PARTITION_TBLS.hql > $outputTargetPath/4_COUNT_NON_PARTITION_TBLS.hql
sed "s/$oldschemaname/$newschemaname/g" $outputSourcePath/3_COUNT_PARTITIONS_TBLS.hql > $outputTargetPath/5_COUNT_PARTITIONS_TBLS.hql

