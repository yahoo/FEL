

## Mine Wikipedia and Extract Graph-Based Counts
The tool makes use of a datapack that stores counts and aliases (mentions) of entities from different sources. Originally,
we used anchor text and query logs. The following describes how to mine and compute the anchor text from a public Wikipedia dump using a hadoop cluster (or if
there is not one, you can use hadoop in a single machine). This is based on the code from the [Cloud9](https://lintool.github.io/Cloud9/) toolkit.

The main classes involved are WikipediaDocnoMappingBuilder, RepackWikipedia, ExtractWikipediaAnchorText and Datapack. The final datapack is
created using three pig scripts.

* WikipediaDocnoMappingBuilder: filters the Wiki pages from the dump and records the ids of the entity pages
* RepackWikipedia: rewrites the Wikipedia dump using the entity pages only
* ExtractWikipediaAnchorText: extracts the in-wiki links from Wikipedia and creates pairs (alias, entity) where the alias is the text in the anchor
    and the entity is the id of a Wikipedia page pointed out by an outgoing link.
* Datapack: creates a first version of the datapack (with no aggregate counts).

The pig scripts are used to aggregate counts for aliases and entities, and rewrite the final datapack.
The detailed commands you need to run to create the datapack come next.


Download Latest Wikipedia Dump

```bash
# set WIKI_MARKET to the wikipedia dump language, e.g.
WIKI_MARKET=en

# set WIKI_DATE to the wikipedia dump you want to process, e.g.
WIKI_DATE=20151102

#set your WORKING_DIR to the directory you want to store the data
WORKING_DIR=/grid/tmp/wiki

# create directory
mkdir --parents ${WORKING_DIR}/${WIKI_MARKET}/${WIKI_DATE}

# download datapack from web
 wget --output-document=${WORKING_DIR}/${WIKI_MARKET}/${WIKI_DATE}/pages-articles.xml.bz2 \
http://dumps.wikimedia.org/\
${WIKI_MARKET}wiki/${WIKI_DATE}/${WIKI_MARKET}wiki-${WIKI_DATE}-pages-articles.xml.bz2

# unzip datapack
bzip2 --verbose --keep --decompress \
${WORKING_DIR}/${WIKI_MARKET}/${WIKI_DATE}/pages-articles.xml.bz2

# create hdfs directory
hdfs dfs -mkdir -p wiki/${WIKI_MARKET}/${WIKI_DATE}

# copy datapack on hdfs
hdfs dfs -copyFromLocal \
${WORKING_DIR}/${WIKI_MARKET}/${WIKI_DATE}/pages-articles.xml \
wiki/${WIKI_MARKET}/${WIKI_DATE}/
```

Preprocess Datapack
```bash
hadoop \
jar target/FEL-0.1.0.jar \
com.yahoo.semsearch.fastlinking.io.WikipediaDocnoMappingBuilder \
-Dmapreduce.map.env="JAVA_HOME=/home/gs/java/jdk64/current" \
-Dmapreduce.reduce.env="JAVA_HOME=/home/gs/java/jdk64/current" \
-Dyarn.app.mapreduce.am.env="JAVA_HOME=/home/gs/java/jdk64/current" \
-Dmapred.job.map.memory.mb=6144 \
-Dmapreduce.map.memory.mb=6144 \
-Dmapred.child.java.opts="-Xmx2048m" \
-Dmapreduce.map.java.opts='-Xmx2g -XX:NewRatio=8 -XX:+UseSerialGC' \
-input wiki/${WIKI_MARKET}/${WIKI_DATE}/pages-articles.xml \
-output_file wiki/${WIKI_MARKET}/${WIKI_DATE}/docno.dat \
-wiki_language ${WIKI_MARKET} \
-keep_all

hadoop \
jar target/FEL-0.1.0.jar \
com.yahoo.semsearch.fastlinking.io.RepackWikipedia \
-Dmapreduce.map.env="JAVA_HOME=/home/gs/java/jdk64/current" \
-Dmapreduce.reduce.env="JAVA_HOME=/home/gs/java/jdk64/current" \
-Dyarn.app.mapreduce.am.env="JAVA_HOME=/home/gs/java/jdk64/current" \
-Dmapred.job.map.memory.mb=6144 \
-Dmapreduce.map.memory.mb=6144 \
-Dmapred.child.java.opts="-Xmx2048m" \
-Dmapreduce.map.java.opts='-Xmx2g -XX:NewRatio=8 -XX:+UseSerialGC' \
-input wiki/${WIKI_MARKET}/${WIKI_DATE}/pages-articles.xml \
-mapping_file wiki/${WIKI_MARKET}/${WIKI_DATE}/docno.dat \
-output wiki/${WIKI_MARKET}/${WIKI_DATE}/pages-articles.block \
-wiki_language ${WIKI_MARKET} \
-compression_type block
```

Build Data Structures and extract anchor text
```bash
hadoop \
jar target/FEL-0.1.0.jar\
com.yahoo.semsearch.fastlinking.io.ExtractWikipediaAnchorText \
-Dmapreduce.map.env="JAVA_HOME=/home/gs/java/jdk64/current" \
-Dmapreduce.reduce.env="JAVA_HOME=/home/gs/java/jdk64/current" \
-Dyarn.app.mapreduce.am.env="JAVA_HOME=/home/gs/java/jdk64/current" \
-Dmapred.job.map.memory.mb=6144 \
-Dmapreduce.map.memory.mb=6144 \
-Dmapred.child.java.opts="-Xmx2048m" \
-Dmapreduce.map.java.opts='-Xmx2g -XX:NewRatio=8 -XX:+UseSerialGC' \
-input wiki/${WIKI_MARKET}/${WIKI_DATE}/pages-articles.block \
-emap wiki/${WIKI_MARKET}/${WIKI_DATE}/entities.map \
-amap wiki/${WIKI_MARKET}/${WIKI_DATE}/anchors.map \
-cfmap wiki/${WIKI_MARKET}/${WIKI_DATE}/alias-entity-counts.map \
-redir wiki/${WIKI_MARKET}/${WIKI_DATE}/redirects
```

Compute anchor text counts

```bash
hadoop \
jar target/FEL-0.1.0.jar \
com.yahoo.semsearch.fastlinking.io.Datapack \
-amap wiki/${WIKI_MARKET}/${WIKI_DATE}/anchors.map \
-cfmap wiki/${WIKI_MARKET}/${WIKI_DATE}/alias-entity-counts.map \
-multi true \
-output ${WORKING_DIR}/${WIKI_MARKET}/${WIKI_DATE}/alias-entity-counts

# copy to hdfs
hadoop dfs -copyFromLocal \
${WORKING_DIR}/${WIKI_MARKET}/${WIKI_DATE}/alias-entity-counts.dat \
wiki/${WIKI_MARKET}/${WIKI_DATE}/

# copy to hdfs
hadoop dfs -copyFromLocal \
${WORKING_DIR}/${WIKI_MARKET}/${WIKI_DATE}/alias-entity-counts.tsv \
wiki/${WIKI_MARKET}/${WIKI_DATE}/

# create directory
hadoop dfs -mkdir -p \
wiki/${WIKI_MARKET}/${WIKI_DATE}/feat/alias-entity/count

# copy counts
hadoop dfs -copyFromLocal ${WORKING_DIR}/${WIKI_MARKET}/${WIKI_DATE}/alias-entity-counts.tsv \
wiki/${WIKI_MARKET}/${WIKI_DATE}/feat/alias-entity/count

# set numerical id
hadoop dfs -text wiki/${WIKI_MARKET}/${WIKI_DATE}/feat/alias-entity/count/* | \
cut --fields 4 | \
LC_ALL=C sort --dictionary-order | \
LC_ALL=C uniq | \
awk '{print $0"\t"NR}' | \
hadoop dfs -put - wiki/${WIKI_MARKET}/${WIKI_DATE}/id-entity.tsv
```

Aggregate Alias-Entity Dependent Counts
```bash
# set FEL_DATE to the FEL datapack generation date, e.g.
FEL_DATE=20151108

# set WIKI_MARKET to the wikipedia dump language, e.g.
WIKI_MARKET=en

# set WIKI_DATE to the wikipedia dump you want to process, e.g.
WIKI_DATE=20151102

pig \
-stop_on_failure \
-Dpig.additional.jars=./target/FEL-0.1.0.jar \
-Dmapred.output.compression.enabled=true \
-Dmapred.output.compress=true \
-Dmapred.output.compression.type=BLOCK \
-Dmapred.output.compression.codec=org.apache.hadoop.io.compress.BZip2Codec \
-Dmapred.job.queue.name=adhoc \
-param feat=wiki/${WIKI_MARKET}/${WIKI_DATE}/feat/alias-entity/count \
-param output=fel/${FEL_DATE}/feat/graph/${WIKI_MARKET}/${WIKI_DATE}/alias-entity/agg \
-file ./src/main/pig/aggregate-graph-alias-entity-counts.pig
```


```bash
pig \
-stop_on_failure \
-Dmapred.output.compression.enabled=true \
-Dmapred.output.compress=true \
-Dmapred.output.compression.type=BLOCK \
-Dmapred.output.compression.codec=org.apache.hadoop.io.compress.BZip2Codec \
-Dmapred.job.queue.name=adhoc \
-param counts=fel/${FEL_DATE}/feat/graph/${WIKI_MARKET}/${WIKI_DATE}/alias-entity/agg \
-param output=fel/${FEL_DATE}/feat/graph/${WIKI_MARKET}/${WIKI_DATE}/alias-entity/final \
-file ./src/main/pig/compute-graph-alias-entity-counts.pig
```

Generate Feature Vectors

This script would merge Search-Based Counts and Graph-Based Counts (or different counts) - currently the search based
counts are set to zero.

```bash
# set FEL_DATE to the FEL datapack generation date, e.g.
FEL_DATE=20151108

# set WIKI_MARKET to the wikipedia dump language, e.g.
WIKI_MARKET=en

# set WIKI_DATE to the wikipedia dump you want to process, e.g.
WIKI_DATE=20151102

time \
pig \
-stop_on_failure \
-useHCatalog \
-Dpig.additional.jars=./target/FEL-0.1.0.jar \
-Dmapred.output.compression.enabled=true \
-Dmapred.output.compress=true \
-Dmapred.output.compression.type=BLOCK \
-Dmapred.output.compression.codec=org.apache.hadoop.io.compress.BZip2Codec \
-Dmapred.job.queue.name=adhoc \
-param entity=wiki/${WIKI_MARKET}/${WIKI_DATE}/id-entity.tsv \
-param graph=fel/${FEL_DATE}/feat/graph/${WIKI_MARKET}/${WIKI_DATE}/alias-entity/final \
-param output=fel/${FEL_DATE}/feat/all/${WIKI_MARKET}/final \
-param err_output=fel/${FEL_DATE}/feat/all/${WIKI_MARKET}/log/final \
-file ./src/main/pig/join-alias-entity-counts.pig \
>& join-alias-entity-counts.log
```

Copy Datapack to Local Directory

```bash
OUTPUT_DIR=\
${WORKING_DIR}/fel/datapack/${FEL_DATE}/\
wiki-${WIKI_MARKET}-${WIKI_DATE}

mkdir --parent ${OUTPUT_DIR}

hadoop dfs -copyToLocal \
wiki/${WIKI_MARKET}/${WIKI_DATE}/id-entity.tsv \
${OUTPUT_DIR}/

hadoop dfs -text \
fel/${FEL_DATE}/feat/all/${WIKI_MARKET}/final/* | \
sed "s/\t{(/\t/" | \
sed "s/),(/\t/g" | \
sed "s/)}$//" \
> ${OUTPUT_DIR}/features.dat

chmod --recursive ugo+rx \
${WORKING_DIR}/fel
```
