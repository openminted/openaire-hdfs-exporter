# openaire-hdfs-exporter


#### compile with
mvn assembly:assembly

#### execute with
java -jar target/tar-hdfs-exporter-jar-with-dependencies.jar -f [configurationFile] -s 1832000 -z 50

Optional options:

-v verbose\
-s starting point\
-t number of total files to fetch\
-z number of files to zip


#### Example configuration:

hdfs.exporter.seq.file			=	hdfs://quorum1.t.hadoop.research-infrastructures.eu:8020/tmp/indexrecords_db_openaireplus_node1.seq
hdfs.exporter.out.dir			=	/tmp/hdfsOut

#CORE-SITE

fs.defaultFS					=	hdfs://quorum1.t.hadoop.research-infrastructures.eu:8020
hadoop.security.authentication	=	simple
hadoop.security.auth_to_local	=	DEFAULT

#HDFS-SITE

dfs.https.port					=	50470
dfs.namenode.https-address		=	quorum1.t.hadoop.research-infrastructures.eu:50470
dfs.namenode.http-address		=	quorum1.t.hadoop.research-infrastructures.eu:50070
