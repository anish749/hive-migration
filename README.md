# Hive Migration across Hadoop Clusters

This repository talks of Hive schema migration from one Hadoop cluster to another, or from one Hive 
version to another.

The process of migration is to get a dump of all Hive queries which can then be run in the target
installation as Hive scripts. This includes Hive DB creation and Table creation DDL Scripts, as 
well as adding partition information to these tables.

Please refer to the manual for understanding and using the queries.

* Tested on Hive v0.13 as Source and MySQL as MetaStore DB

This approach is well suited when the following drawbacks are encountered:

	i) 	The IMPORT-EXPORT command doesn't work: 
		For tables with no partition, the export command creates a data folder. The import command
		will point to the new location, but will have the folder "data" in it. This can be a problem
		if a separate program is writing data to the location from where the Hive table is pointing/
		reading.
	
	ii)	MSCK REPAIR TABLE doesn't work:
		If MR jobs has multiple outputs configured and the outputs are to be added as partitions for
		more than one Hive table, then the MSCK Repair table would not be able to get the correct HDFS
		paths added.
		This also fails when the partition locations are customized.
	
	iii)	Hive Metastore Dump from Source to Target:
		If there is a lot of change in Hive Metastore Schema and there is a version upgrade along with
		cluster migration. Also it might be a little risky since we are touching the raw metadata.
		It is however the best way if the versions are same.

Drawbacks of this approach:

	i) Indexes are NOT migrated

