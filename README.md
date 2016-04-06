# FEL
Fast Entity Linker Core

This library performs query segmentation and entity linking to a target reference Knowledge Base (i.e, Wikipedia). In its current version is tailored
+towards query entity linking (alternatively, short fragments of text). The main goal was to have an extremely fast linker
 (< 1 or 2 ms/query on average on a standard laptop) that is completely unsupervised, so that more sophisticated approaches can work on top of it with a decent time budget available. A side effect of this is that the datapack used by the linker
occupies <3GB making it suitable to run on the grid (and making the footprint on server machines very low).

##Install

The project comes with a pom.xml which should install all the dependencies required, except for one that has to be installed manually:
```bash
git clone https://github.com/ot/entity2vec; cd entity2vec; mvn install
```
Then
```bash
mvn clean; mvn package
```

## What does this tool do?
The library performs query and document entity linking. It implements different algorithms that return a confidence score (~log likelihood)
that should be (more or less) comparable across pieces of text of different length so one can use a global threshold for linking. The program operates
with two datastructures, one big hash and compressed word and entity vectors. The hash is generated out of a datapack that records __counts__ of
phrases and entities that co-ocur together. This counts might come from different sources, for instance anchor text and query logs. In anchor
text, whenever there's a link to a corresponding entity page we would store the anchor and the entity counds. In a query log whenever there's a
click to an entity page, we would update the query and entity counts. The word and entity vector files are compressed vector (duh) representations
that account for the contexts in which the word/entity appears. Word vectors can be generated using general tools like word2vec, whereas the library
provides a way to learn the entity vectors.

The library also comes with two different sets of tools for generating the hash and the word vector files.
* First, you need to generate a __datapack__ that stores counts of phrases and entities. We provide tools for mining a Wikipedia dump and creating the datapack out of it.
* Generating a hash structure out of a datapack.
* Generating entity vectors out of a set of entity descriptions, which can be extracted from Wikipedia pages.
* Compressing word vectors (typical compression ratios are around 10x).

If you use this library, please cite the following paper:

@inproceedings{Blanco:WSDM2015,
        Address = {New York, NY, USA},
        Author = {Blanco, Roi and Ottaviano, Giuseppe and Meij, Edgar},
        Booktitle = {Proceedings of the Eight ACM International Conference on Web Search and Data Mining},
        Location = {Shanghai, China},
        Numpages = {10},
        Publisher = {ACM},
        Series = {WSDM '15},
        Title = {Fast and Space-Efficient Entity Linking in Queries},
        Year = {2015},
}


#### Stand-alone query entity linking

There are a number of different rankers/linkers that use different conceptual models. The overall description of the algorithm with some implementation details is at:

[http://www.dc.fi.udc.es/~roi/publications/wsdm2015.pdf] 

The two main classes to use are 
com.yahoo.semsearch.fastlinking.FastEntityLinker (no context)
com.yahoo.semsearch.fastlinking.EntityContextFastEntityLinker (context-aware)

The classes can be called with --help for the input option list.
They provide interactive linking through stdin (edit the code or extend for custom output format).

Example usage calls (you don't need rlwrap but it's nice to have):
```bash
rlwrap java -Xmx10G com.yahoo.semsearch.fastlinking.EntityContextFastEntityLinker -h data/alias.hash -u data/PHRASE.model -e data/ENTITIES.PHRASE.model

rlwrap java -Xmx10G com.yahoo.semsearch.fastlinking.FastEntityLinker data/alias.hash
```
#### Grid based linking
The following command works 
```bash
    hadoop jar FEL-0.1.0-fat.jar \
    -Dmapred.map.tasks=100 \
    -Dmapreduce.map.java.opts=-Xmx3g \
    -Dmapreduce.map.memory.mb=3072 \
    -Dmapred.job.queue.name=adhoc \
    -files /grid/0/tmp/roi/hashfile#hash,/grid/0/tmp/roi/id-type.tsv#mapping \
    <inputfile>
    <outputfile>
```
The jar reads files that have one query per line - it splits on <TAB> and takes the first element. 
At the moment it outputs the following format (which is PIG-friendly) although it could print out anything else if needed to. 

(a beka summer books,{(summer,Summer,1768785,678,-4.572464942932129)(books,Book,1882136,998,-5.444955348968506)(a beka,A_Beka_Book,2889283,926,-2.051509857177734)})    1
(air cooled gasoline engines,{(gasoline engines,Gas_engine,12299268,0,-2.340573310852051)(air cooled,Air-cooled_engine,2494691,15,-1.2365872859954834)})        1

This is:
 (query, {(span, entity_yk_id, entity_fnel_id(ignore), entity_type, entity_score)} ) (entities are -not- ordered by score).

In general you should rely on thresholding and possibly sticking to the top-1 entity retrieved but this depends on how you are going to use it.


#### Fiddling with word embeddings


Trains and quantizes word2vec vectors for uni/bigrams and entities. An entity is anything that has 1) an identifier 2) a sequence of words describing it - a document, sentence could match 
this definition.

JAVA VERSION

--word vectors
First, compute word embeddings using whatever software you prefer, and output the word vectors in word2vec C format.

To quantize and compress the vectors:
```bash
java com.yahoo.semsearch.fastlinking.w2v.Quantizer -i <word_embeddings> -o <output> -h

java -Xmx5G com.yahoo.semsearch.fastlinking.w2v.Word2VecCompress <quantized_file> <output>
```
--entity vectors
The program accepts a file with the following format (one per line)

entity_id <TAB> number <TAB> word sequence

You can generate this file out of a Wikipedia dump using
```bash
java com.yahoo.semsearch.fastlinking.utils.ExtractFirstParagraphs <input_wiki_dump> <output_file>
```

The class com.yahoo.semsearch.fastlinking.w2v.EntityEmbeddings computes entity embeddings (on the grid or on a single machine).

```bash
hadoop jar FEL-0.1.0-fat.jar com.yahoo.semsearch.fastlinking.w2v.EntityEmbeddings -Dmapreduce.input .fileinputformat.split.minsize=100 -Dmapreduce.input.fileinputformat.split.maxsize=90000 -Dmapreduce.job.maps=3000
-Dmapred.child.java.opts=-XX:-UseGCOverheadLimit -Dmapreduce.map.java.opts=-Xmx3g -Dmapreduce.map.memory.mb=3072 \
-Dmapreduce.job.queuename=adhoc \ -files /grid/0/tmp/roi/vectors#vectors E2W.text entity.embeddings
```


Then, you can quantize and compress the resulting vectors, just like we did for word embeddings.

If you have a large number (likely) of entity vectors, it might take a while to find the right quantization factor, and you might want to use one straight ahead:
```bash
java com.yahoo.semsearch.fastlinking.w2v.Quantizer -i <embeddings> -o <output> -d 8 
```
To compress the vectors, it's better to use the following class (it scales up to millions of vectors):
```bash
com.yahoo.semsearch.fastlinking.w2v.EfficientWord2VecCompress
```


## Mine Wikipedia and Extract Graph-Based Counts

The minimum steps required to mine and compute the entity dependent features, the alias-entity dependent counts and the entity redirects are described below. 

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
com.yahoo.bcn.util.WikipediaDocnoMappingBuilder \
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
com.yahoo.bcn.util.RepackWikipedia \
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

Build Data Structures
```bash
hadoop \
jar target/FEL-0.1.0.jar\
com.yahoo.bcn.ExtractWikipediaAnchorText \
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

Compute Alias-Entity Dependent Counts

```bash
hadoop \
jar target/wikipedia-extraction-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
com.yahoo.bcn.AlpDatapack \
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

###Compute Graph-Based Entity Dependent Features & Alias-Entity Dependent Features

The minimum steps required to compute the alias-entity dependent features are described below. Please refer to the following maven project for more details:

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
``
##Compute Alias-Entity Dependent Features

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

##Generate Features Vectors

Merge Search-Based Counts and Graph-Based Counts
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
``
