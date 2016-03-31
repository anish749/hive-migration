source source_wrapper.properties
#Run count rows query in source side

#Hive Script Path for source Hive Installation Scripts
outputSourcePath=$(pwd)/generatedSQL_source_Hive

#Hive Script Path for target Hive Installation Scripts
outputTargetPath=$(pwd)/generatedSQL_target_Hive

hive -f $outputSourcePath/2_COUNT_NON_PARTITION_TBLS.hql > $outputSourcePath/count_non_part.txt
ret=$?
if [ $ret -ne 0 ];
then
        echo "Error - Hive Error code is $ret while trying to run count queries on non partitioned tables"
        exit $ret
else
        echo "COUNT non partitioned tables completed. Output stored in $outputSourcePath/count_non_part.txt"
fi

hive -f $outputSourcePath/3_COUNT_PARTITIONS_TBLS.hql > $outputSourcePath/count_part.txt
ret=$?
if [ $ret -ne 0 ];
then
        echo "Error - Hive Error code is $ret while trying to run count queries on partitioned tables"
        exit $ret
else
        echo "COUNT partitioned tables completed. Output stored in $outputSourcePath/count_part.txt"
fi


