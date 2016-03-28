source source_wrapper.properties

#Hive Script Path for source Hive Installation Scripts
outputSourcePath=$(pwd)/generatedSQL_source_Hive
rm -rf $outputSourcePath
mkdir -p $outputSourcePath

#Hive Script Path for target Hive Installation Scripts
outputTargetPath=$(pwd)/generatedSQL_target_Hive
rm -rf $outputTargetPath
mkdir -p $outputTargetPath

# Extract Create DB statemenets from MetaStore to Run in Target Hive Installation.

mysql -h$MYSQL_SERVER_HOST -u $HIVE_METASTORE_USER -p$HIVE_METASTORE_PASS -D$HIVE_METASTORE_SCHEMA --skip-column-names -e"
set @SCHEMA_TO_MIGRATE = '$SCHEMA_TO_MIGRATE';

-- Query for creating databases
SELECT 
    CONCAT('CREATE DATABASE IF NOT EXISTS ',
            D.NAME,
            '\;') AS CREATE_DB_STATEMENTS
FROM
    DBS D
WHERE
    D.name LIKE @SCHEMA_TO_MIGRATE;" > $outputTargetPath/1_DBS.hql

ret=$?
if [ $ret -ne 0 ];
then
	echo "Error - MySQL Error code is $ret while trying to extract DB creation scripts"
	exit $ret
else
	echo "DB Creation extraction script completed successfully"
fi




# Extract Show CT statemenets from MetaStore to Run in Source Hive Installation.

mysql -h$MYSQL_SERVER_HOST -u $HIVE_METASTORE_USER -p$HIVE_METASTORE_PASS -D$HIVE_METASTORE_SCHEMA --skip-column-names -e"
set @SCHEMA_TO_MIGRATE = '$SCHEMA_TO_MIGRATE';

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
        AND D.name LIKE @SCHEMA_TO_MIGRATE;" > $outputSourcePath/1_Show_CT.hql

ret=$?
if [ $ret -ne 0 ];
then
	echo "Error - MySQL Error code is $ret while trying to extract SHOW CT scripts"
	exit $ret
else
	echo "SHOW CT extraction script completed successfully"
fi

echo "Logging into Source Hive Installation" 
# Run Show CT Scripts on Source Hive Installation and create target Hive CT scripts
hive -f $outputSourcePath/1_Show_CT.hql > $outputTargetPath/2_CT_temp.hql
ret=$?
if [ $ret -ne 0 ];
then
	echo "Error - Hive Error code is $ret while trying to execute SHOW CT scripts"
	exit $ret
else
	echo "SHOW CT execution script completed successfully"
fi

# Add ; after CT statements
cat $outputTargetPath/2_CT_temp.hql | tr '\n' '\f' | sed -e 's/)\fCREATE/);\fCREATE/g'  | tr '\f' '\n' > $outputTargetPath/2_CT.hql
echo ";">>$outputTargetPath/2_CT.hql
rm -rf $outputTargetPath/2_CT_temp.hql


# Rename schema names in CT statements
# sed -i 's/oldschemaname/newschemaname/g' $outputTargetPath/2_CT.hql

# Replace Name Node IP addresses
echo "Replacing $oldNN with $newNN in CT scripts"
sed -i "s/$oldNN/$newNN/g" $outputTargetPath/2_CT.hql


# Extract Add Partition statemenets from MetaStore to Run in Target Hive Installation.

mysql -h$MYSQL_SERVER_HOST -u $HIVE_METASTORE_USER -p$HIVE_METASTORE_PASS -D$HIVE_METASTORE_SCHEMA --skip-column-names -e"
-- Query to create add partition scripts to run in target Hive Installation

set @SCHEMA_TO_MIGRATE = '$SCHEMA_TO_MIGRATE';
SET @prevDB = 'nodbxyzabc_0b040aca_d613_4c47_9e2c_b21daafba13f'; -- Note - this DB should not exist in the source Hive Installation
SET @oldNameNode = '$oldNN'; -- Old NameNode address
SET @newNameNode = '$newNN'; -- New NameNode address

SELECT 
    REPLACE(add_partitions_script,
        @oldNameNode,
        @newNameNode) AS add_partitions_script
FROM
    (SELECT 
        @prevDB prev,
            @prevDB:=D.Name curr,
            CASE
                WHEN
                    @prevDB <> D.Name
                THEN
                    CONCAT('USE ', D.name, '\; ', ' ALTER TABLE ', T.TBL_NAME, ' ADD PARTITION (', GROUP_CONCAT(PK.PKEY_NAME, '=', '\'', PKV.PART_KEY_VAL, '\''
                        ORDER BY PK.INTEGER_IDX), ') LOCATION \'', S.location, '\'; ')
                ELSE CONCAT(' ALTER TABLE ', T.TBL_NAME, ' ADD PARTITION (', GROUP_CONCAT(PK.PKEY_NAME, '=', '\'', PKV.PART_KEY_VAL, '\''
                    ORDER BY PK.INTEGER_IDX), ') LOCATION \'', S.location, '\'\; ')
            END AS add_partitions_script
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
    ORDER BY D.name) alias1;" > $outputTargetPath/3_ADD_PARTITION.hql
ret=$?
if [ $ret -ne 0 ];
then
	echo "Error - MySQL Error code is $ret while trying to extract ADD PARTITION scripts"
	exit $ret
else
	echo "ADD PARTITION extraction script completed successfully"
fi	

echo "Target Hive installation scripts placed at $outputTargetPath"

exit 0

