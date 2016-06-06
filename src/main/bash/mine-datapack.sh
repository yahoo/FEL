##@roiblanco
# set WIKI_MARKET to the wikipedia dump language, e.g.
WIKI_MARKET=en
# set WIKI_DATE to the wikipedia dump you want to process, e.g.
WIKI_DATE=20160407
#set your WORKING_DIR to the directory you want to store the data
WORKING_DIR=/grid/0/tmp/roi/wiki
#Max hadoop node memory 
MAPRED_MEM=1700
HADOOP_MEM=1700
#PIG script folder
PIG_FOLDER=/homes/roi/FEL/src/main/pig
FELJAR_FOLDER=/homes/roi/FEL/target
FEL_DATE=${WIKI_DATE}
FEL_JAR=FEL-0.1.0-fat.jar
HASH_NAME=${WORKING_DIR}/fel/hash.${WIKI_DATE}

if [ "$1" = "dl" ] ; then
	# create directory
	mkdir --parents ${WORKING_DIR}/${WIKI_MARKET}/${WIKI_DATE}
	# download datapack from web
	 wget --output-document=${WORKING_DIR}/${WIKI_MARKET}/${WIKI_DATE}/pages-articles.xml.bz2 \
	http://dumps.wikimedia.org/\
	${WIKI_MARKET}wiki/${WIKI_DATE}/${WIKI_MARKET}wiki-${WIKI_DATE}-pages-articles.xml.bz2
	# unzip datapack
	bzip2 --verbose --keep --decompress \
	${WORKING_DIR}/${WIKI_MARKET}/${WIKI_DATE}/pages-articles.xml.bz2
 	#create hdfs directory
	hadoop fs -mkdir -p wiki/${WIKI_MARKET}/${WIKI_DATE}
	# copy datapack on hdfs
	hadoop fs -copyFromLocal \
	${WORKING_DIR}/${WIKI_MARKET}/${WIKI_DATE}/pages-articles.xml \
	wiki/${WIKI_MARKET}/${WIKI_DATE}/

elif  [ "$1" = "preprocess" ]; then 
	hadoop \
	jar ${FELJAR_FOLDER}/${FEL_JAR} \
	com.yahoo.semsearch.fastlinking.io.WikipediaDocnoMappingBuilder \
	-Dmapreduce.map.env="JAVA_HOME=/home/gs/java/jdk64/current" \
	-Dmapreduce.reduce.env="JAVA_HOME=/home/gs/java/jdk64/current" \
	-Dyarn.app.mapreduce.am.env="JAVA_HOME=/home/gs/java/jdk64/current" \
	-Dmapred.job.map.memory.mb=${MAPRED_MEM} \
	-Dmapreduce.map.memory.mb=${MAPRED_MEM} \
	-Dmapred.child.java.opts="-Xmx${HADOOP_MEM}m" \
	-Dmapreduce.map.java.opts="-Xmx${HADOOP_MEM}m -XX:NewRatio=8 -XX:+UseSerialGC" \
	-input wiki/${WIKI_MARKET}/${WIKI_DATE}/pages-articles.xml \
	-output_file wiki/${WIKI_MARKET}/${WIKI_DATE}/docno.dat \
	-wiki_language ${WIKI_MARKET} \
	-keep_all || { echo 'WikipediaDocnoMappingBuilder failed' ; exit 1; }

	hadoop \
	jar ${FELJAR_FOLDER}/${FEL_JAR} \
	com.yahoo.semsearch.fastlinking.io.RepackWikipedia \
	-Dmapreduce.map.env="JAVA_HOME=/home/gs/java/jdk64/current" \
	-Dmapreduce.reduce.env="JAVA_HOME=/home/gs/java/jdk64/current" \
	-Dyarn.app.mapreduce.am.env="JAVA_HOME=/home/gs/java/jdk64/current" \
	-Dmapred.job.map.memory.mb=${MAPRED_MEM} \
	-Dmapreduce.map.memory.mb=${MAPRED_MEM} \
	-Dmapred.child.java.opts="-Xmx${HADOOP_MEM}m" \
	-Dmapreduce.map.java.opts="-Xmx${HADOOP_MEM}m -XX:NewRatio=8 -XX:+UseSerialGC" \
	-input wiki/${WIKI_MARKET}/${WIKI_DATE}/pages-articles.xml \
	-mapping_file wiki/${WIKI_MARKET}/${WIKI_DATE}/docno.dat \
	-output wiki/${WIKI_MARKET}/${WIKI_DATE}/pages-articles.block \
	-wiki_language ${WIKI_MARKET} \
	-compression_type block || { echo 'RepackWikipedia failed' ; exit 1; }

elif [ "$1" = "anchor" ]; then
	hadoop \
	jar ${FELJAR_FOLDER}/${FEL_JAR} \
	com.yahoo.semsearch.fastlinking.io.ExtractWikipediaAnchorText \
	-Dmapreduce.map.env="JAVA_HOME=/home/gs/java/jdk64/current" \
	-Dmapreduce.reduce.env="JAVA_HOME=/home/gs/java/jdk64/current" \
	-Dyarn.app.mapreduce.am.env="JAVA_HOME=/home/gs/java/jdk64/current" \
	-Dmapred.job.map.memory.mb=${MAPRED_MEM} \
	-Dmapreduce.map.memory.mb=${MAPRED_MEM} \
        -Dmapreduce.reduce.memory.mb=${MAPRED_MEM} \
	-Dmapred.child.java.opts="-Xmx${HADOOP_MEM}m" \
	-Dmapreduce.map.java.opts="-Xmx${HADOOP_MEM}m -XX:NewRatio=8 -XX:+UseSerialGC" \
        -Dmapreduce.reduce.java.opts="-Xmx${HADOOP_MEM}m -XX:NewRatio=8 -XX:+UseSerialGC" \
	-input wiki/${WIKI_MARKET}/${WIKI_DATE}/pages-articles.block \
	-emap wiki/${WIKI_MARKET}/${WIKI_DATE}/entities.map \
	-amap wiki/${WIKI_MARKET}/${WIKI_DATE}/anchors.map \
	-cfmap wiki/${WIKI_MARKET}/${WIKI_DATE}/alias-entity-counts.map \
	-redir wiki/${WIKI_MARKET}/${WIKI_DATE}/redirects  || { echo 'ExtractWikipediaAnchorText failed' ; exit 1; }

	##correction for multiple reducers at task2 - if you hadoop config can handle a single reducer in task2 this can be ignored

#	hadoop fs -cat wiki/${WIKI_MARKET}/${WIKI_DATE}/entities.map/part*/data |hadoop fs -put - wiki/${WIKI_MARKET}/${WIKI_DATE}/entities.map/singleentitymap.data

     	hadoop \
        jar ${FELJAR_FOLDER}/${FEL_JAR} \
        com.yahoo.semsearch.fastlinking.io.ExtractWikipediaAnchorText \
        -Dmapreduce.map.env="JAVA_HOME=/home/gs/java/jdk64/current" \
        -Dmapreduce.reduce.env="JAVA_HOME=/home/gs/java/jdk64/current" \
        -Dyarn.app.mapreduce.am.env="JAVA_HOME=/home/gs/java/jdk64/current" \
        -Dmapred.job.map.memory.mb=6144 \
        -Dmapreduce.map.memory.mb=6144 \
        -Dmapreduce.reduce.memory.mb=6144 \
        -Dmapred.child.java.opts="-Xmx2048m" \
        -Dmapreduce.map.java.opts="-Xmx2048m" \
	-Dmapreduce.reduce.java.opts="-Xmx2048m" \
        -input wiki/${WIKI_MARKET}/${WIKI_DATE}/pages-articles.block \
        -emap wiki/${WIKI_MARKET}/${WIKI_DATE}/entities.map \
        -amap wiki/${WIKI_MARKET}/${WIKI_DATE}/anchors.map \
        -cfmap wiki/${WIKI_MARKET}/${WIKI_DATE}/alias-entity-counts.map \
	-phase 2 \
        -redir wiki/${WIKI_MARKET}/${WIKI_DATE}/redirects  || { echo 'ExtractWikipediaAnchorText failed' ; exit 1; }


       hadoop \
	 jar ${FELJAR_FOLDER}/${FEL_JAR} \
	 com.yahoo.semsearch.fastlinking.io.ExtractWikipediaAnchorText \
	 -Dmapreduce.map.env="JAVA_HOME=/home/gs/java/jdk64/current" \
	  -Dmapreduce.reduce.env="JAVA_HOME=/home/gs/java/jdk64/current" \
	 -Dyarn.app.mapreduce.am.env="JAVA_HOME=/home/gs/java/jdk64/current" \
	 -Dmapred.job.map.memory.mb=6144 \
	 -Dmapreduce.map.memory.mb=6144 \
         -Dmapreduce.reduce.memory.mb=6144 \
	 -Dmapred.child.java.opts="-Xmx2048m" \
	 -Dmapreduce.map.java.opts="-Xmx2048m" \
         -Dmapreduce.reduce.java.opts="-Xmx2048m" \
	 -input wiki/${WIKI_MARKET}/${WIKI_DATE}/pages-articles.block \
	 -emap wiki/${WIKI_MARKET}/${WIKI_DATE}/entities.map \
	 -amap wiki/${WIKI_MARKET}/${WIKI_DATE}/anchors.map \
	 -cfmap wiki/${WIKI_MARKET}/${WIKI_DATE}/alias-entity-counts.map \
       -phase 3 \
	 -redir wiki/${WIKI_MARKET}/${WIKI_DATE}/redirects  || { echo 'ExtractWikipediaAnchorText failed' ; exit 1; }

	hadoop \
	jar ${FELJAR_FOLDER}/${FEL_JAR} \
	com.yahoo.semsearch.fastlinking.io.Datapack \
	-amap wiki/${WIKI_MARKET}/${WIKI_DATE}/anchors.map \
	-cfmap wiki/${WIKI_MARKET}/${WIKI_DATE}/alias-entity-counts.map \
	-multi true \
	-ngram false \
	-output ${WORKING_DIR}/${WIKI_MARKET}/${WIKI_DATE}/alias-entity-counts || { echo 'Datapack failed' ; exit 1; }

	# copy to hdfs
	hadoop fs -copyFromLocal \
	${WORKING_DIR}/${WIKI_MARKET}/${WIKI_DATE}/alias-entity-counts.dat \
	wiki/${WIKI_MARKET}/${WIKI_DATE}/

	# copy to hdfs
	hadoop fs -copyFromLocal \
	${WORKING_DIR}/${WIKI_MARKET}/${WIKI_DATE}/alias-entity-counts.tsv \
	wiki/${WIKI_MARKET}/${WIKI_DATE}/

	# create directory
	hadoop fs -mkdir -p \
	wiki/${WIKI_MARKET}/${WIKI_DATE}/feat/alias-entity/count

	# copy counts
	hadoop fs -copyFromLocal ${WORKING_DIR}/${WIKI_MARKET}/${WIKI_DATE}/alias-entity-counts.tsv \
	wiki/${WIKI_MARKET}/${WIKI_DATE}/feat/alias-entity/count

	# set numerical id
	hadoop fs -text wiki/${WIKI_MARKET}/${WIKI_DATE}/feat/alias-entity/count/* | \
	cut --fields 4 | \
	LC_ALL=C sort --dictionary-order | \
	LC_ALL=C uniq | \
	awk '{print $0"\t"NR}' | \
	hadoop fs -put - wiki/${WIKI_MARKET}/${WIKI_DATE}/id-entity.tsv

elif [ "$1" = "agg" ]; then
	pig \
	-stop_on_failure \
	-Dpig.additional.jars=${FELJAR_FOLDER}/${FEL_JAR} \
	-Dmapred.output.compression.enabled=true \
	-Dmapred.output.compress=true \
	-Dmapred.output.compression.type=BLOCK \
	-Dmapred.output.compression.codec=org.apache.hadoop.io.compress.BZip2Codec \
	-Dmapred.job.queue.name=adhoc \
	-param feat=wiki/${WIKI_MARKET}/${WIKI_DATE}/feat/alias-entity/count \
	-param output=fel/${FEL_DATE}/feat/graph/${WIKI_MARKET}/${WIKI_DATE}/alias-entity/agg \
	-file ${PIG_FOLDER}/aggregate-graph-alias-entity-counts.pig || { echo 'aggregate-graph-alias-entity-counts.pig failed' ; exit 1; }
	
	pig \
	-stop_on_failure \
	-Dmapred.output.compression.enabled=true \
	-Dmapred.output.compress=true \
	-Dmapred.output.compression.type=BLOCK \
	-Dmapred.output.compression.codec=org.apache.hadoop.io.compress.BZip2Codec \
	-Dmapred.job.queue.name=adhoc \
	-param counts=fel/${FEL_DATE}/feat/graph/${WIKI_MARKET}/${WIKI_DATE}/alias-entity/agg \
	-param output=fel/${FEL_DATE}/feat/graph/${WIKI_MARKET}/${WIKI_DATE}/alias-entity/final \
	-file ${PIG_FOLDER}/compute-graph-alias-entity-counts.pig || { echo 'compute-graph-alias-entity-counts.pig failed' ; exit 1; }
	
elif [ "$1" = "gen" ]; then 
	time \
	pig \
	-stop_on_failure \
	-useHCatalog \
	-Dpig.additional.jars=${FELJAR_FOLDER}/${FEL_JAR} \
	-Dmapred.output.compression.enabled=true \
	-Dmapred.output.compress=true \
	-Dmapred.output.compression.type=BLOCK \
	-Dmapred.output.compression.codec=org.apache.hadoop.io.compress.BZip2Codec \
	-Dmapred.job.queue.name=adhoc \
	-param entity=wiki/${WIKI_MARKET}/${WIKI_DATE}/id-entity.tsv \
	-param graph=fel/${FEL_DATE}/feat/graph/${WIKI_MARKET}/${WIKI_DATE}/alias-entity/final \
	-param output=fel/${FEL_DATE}/feat/all/${WIKI_MARKET}/final \
	-param err_output=fel/${FEL_DATE}/feat/all/${WIKI_MARKET}/log/final \
	-file ${PIG_FOLDER}/join-alias-entity-counts.pig || { echo 'join-alias-entity-counts.pig failed' ; exit 1; }

elif [ "$1" = "copy" ]; then
	OUTPUT_DIR=${WORKING_DIR}/fel/datapack/${FEL_DATE}/wiki-${WIKI_MARKET}-${WIKI_DATE}

	mkdir --parent ${OUTPUT_DIR}

	hadoop fs -copyToLocal \
	wiki/${WIKI_MARKET}/${WIKI_DATE}/id-entity.tsv \
	${OUTPUT_DIR}/

	hadoop fs -text \
	fel/${FEL_DATE}/feat/all/${WIKI_MARKET}/final/* | \
	sed "s/\t{(/\t/" | \
	sed "s/),(/\t/g" | \
	sed "s/)}$//" \
	> ${OUTPUT_DIR}/features.dat

	chmod --recursive ugo+rx ${WORKING_DIR}/fel
elif [ "$1" = "hash" ]; then
	FEAT_DIR=${WORKING_DIR}/fel/datapack/${FEL_DATE}/wiki-${WIKI_MARKET}-${WIKI_DATE}
	java -Xmx4G -cp ${FELJAR_FOLDER}/${FEL_JAR} com.yahoo.semsearch.fastlinking.hash.QuasiSuccinctEntityHash \
		 -i ${FEAT_DIR}/features.dat -e ${FEAT_DIR}/id-entity.tsv -o ${HASH_NAME} || { echo 'QuasiSuccinctEntityHash failed' ; exit 1; }
else
  echo "Usage: uber.sh ( commands ... )"
  echo "commands:"
  echo "  dl        download wiki version and upload it to the grid"
  echo "  preprocess	 preprocess wiki datapack "
  echo "  anchor 	extract anchor text "	
  echo "  agg		aggregates entity counts after extracting the anchor text"
  echo "  gen		generates the feature vectors"
  echo "  copy		copies the datapack to the local directory"
  echo "  hash		generates the hash"
fi

