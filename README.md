# FEL

__Fast Entity Linker Core__

This library performs query segmentation and entity linking to a target reference Knowledge Base (i.e., Wikipedia). In its current version it is tailored
+towards query entity linking (alternatively, short fragments of text). The main goal was to have an extremely fast linker
 (< 1 or 2 ms/query on average on a standard laptop) that is completely unsupervised, so that more sophisticated approaches can work on top of it with a decent time budget available. A side effect of this is that the datapack used by the linker
occupies <3GB making it suitable to run on the grid (and making the footprint on server machines very low).

## Install

Please install [maven](https://maven.apache.org/) before you run this project. The project comes with a `pom.xml` which should install all dependencies when you run the command `` mvn install ``. 

## What does this tool do?

The library performs query and document entity linking. It implements different algorithms that return a confidence score (~log likelihood)
that should be (more or less) comparable across pieces of text of different length so one can use a global threshold for linking. The program operates
with two datastructures, one big hash and compressed word and entity vectors. The hash is generated out of a datapack that records __counts__ of
phrases and entities that co-occur together. These counts might come from different sources, for instance anchor text and query logs. In anchor
text, whenever there is a link to a corresponding entity page we would store the anchor and the entity counts. In a query log whenever there is a
click to an entity page, we would update the query and entity counts. The word and entity vector files are compressed vector representations
that account for the contexts in which the word/entity appears. The library
provides a way to learn the entity vectors. Word vectors can be generated using general tools like word2vec, or you can reuse pre-trained word vectors such as those available in Facebook's [fastText](https://github.com/facebookresearch/fastText/blob/master/pretrained-vectors.md) project.

The library also comes with two different sets of tools for generating the hash and the word vector files.
* First, you need to generate a __datapack__ that stores counts of phrases and entities. We provide tools for mining a Wikipedia dump and creating the datapack out of it.
* Generating a hash structure out of a datapack.
* Generating entity vectors out of a set of entity descriptions, which can be extracted from Wikipedia pages.
* Compressing word vectors (typical compression ratios are around 10x).

If you use this library, please cite following papers:

```    
@inproceedings{Blanco:WSDM2015,
  Address = {New York, NY, USA},
  Author = {Blanco, Roi and Ottaviano, Giuseppe and Meij, Edgar},
  Booktitle = {Proceedings of the Eight ACM International Conference on Web Search and Data Mining},
  Location = {Shanghai, China},
  Numpages = {10},
  Publisher = {ACM},
  Series = {WSDM 15},
  Title = {Fast and Space-Efficient Entity Linking in Queries},
  Year = {2015}
}

@inproceedings{Pappu:WSDM2017,
  Address = {New York, NY, USA},
  Author = {Pappu, Aasish, and Blanco, Roi, and Mehdad, Yashar and Stent, Amanda, and Thadani, Kapil},
  Booktitle = {Proceedings of the Tenth ACM International Conference on Web Search and Data Mining},
  Location = {Cambridge, UK},
  Numpages = {10},
  Publisher = {ACM},
  Series = {WSDM 17},
  Title = {Lightweight Multilingual Entity Extraction and Linking},
  Year = {2017}
}
```

#### Stand-alone query entity linking

There are a number of different rankers/linkers that use different conceptual models. The overall description of the algorithm with some implementation details is at:

[Fast and space efficient entity linking for queries](http://www.dc.fi.udc.es/~roi/publications/wsdm2015.pdf)

The main class to use is 
`com.yahoo.semsearch.fastlinking.FastEntityLinker`

The class can be called with `--help` to list the available options.
They provide interactive linking through stdin (edit the code or extend the class if you need a custom output format).

First download the dataset from webscope following the links [provided below](#models).

Example usage call:
```bash
mvn exec:java -Dexec.mainClass=com.yahoo.semsearch.fastlinking.FastEntityLinker \
              -Dexec.args="en/english-nov15.hash"
```

#### Coherent Entity Linking for Documents

The `CoherentEntityLinker` class takes entity-mentions and n-best list of entity-links for each entity mention as input. It constructs a lattice from the n-best lists and runs Forward-Backward algorithm.
 * J. Binder, K. Murphy and S. Russell. Space-Efficient Inference in Dynamic Probabilistic Networks. International, Joint Conf. on Artificial Intelligence, 1997.

More coherency algorithms are under experimentation. They will be added in future versions of the code.

```bash
mvn clean compile exec:java \
  -Dexec.mainClass=com.yahoo.semsearch.fastlinking.CoherentEntityLinkerWrapper \
  -Dexec.args="en/enwiki.wiki2vec.d300.compressed en/english-nov15.hash test.txt" \
  -Dexec.classpathScope=compile
```
You can include a [mapping file](src/main/bash/id-type.tsv) in the entity linker arguments (below) that maps integral entity categories to human-readable entity categories. 

#### Grid based linking

The following command would run the linker on a Hadoop grid:

```bash
hadoop jar FEL-0.1.0-fat.jar \
com.yahoo.semsearch.fastlinking.utils.RunFELOntheGrid \
-Dmapred.map.tasks=100 \
-Dmapreduce.map.java.opts=-Xmx3g \
-Dmapreduce.map.memory.mb=3072 \
-Dmapred.job.queue.name=adhoc \
-files en/english-nov15.hash#hash, src/main/bash/id-type.tsv#mapping \
<inputfile> \
<outputfile>
```

The class reads files that have one query per line - it splits on <TAB> and takes the first element. The output format is:

```
entity_type <TAB> query <TAB> modifier <TAB> entity_id
```

where
* `entity_type` is given in the datapack
* `query` is the original query
* `modifier` is the query string when the entity alias is `remove`
* `entity_id` is the retrieved entity

In general you should rely on thresholding and possibly sticking to the top-1 entity retrieved but this depends on how you are going to use it.


#### Fiddling with word embeddings

This package also provides code to
  1. quantize word2vec vectors for uni/bigrams,
  2. compress quantized vectors,
  3. generate word vectors for entities, given a set of words that describe them.

More on this can be found in the [w2v package](src/main/java/com/yahoo/semsearch/fastlinking/w2v/README.md).


#### Mine Wikipedia and Extract Graph-Based Counts

The tool makes use of a datapack that stores counts and aliases (mentions) of entities from different sources. Originally,
we used anchor text and query logs. The following describes how to mine and compute the anchor text from a public Wikipedia dump using a hadoop cluster (or if
there is not one, you can use hadoop in a single machine). This is based on the code from the [Cloud9](https://lintool.github.io/Cloud9/) toolkit.

More on this can be found in the [io package](src/main/java/com/yahoo/semsearch/fastlinking/io/README.md).


####Creating a Quasi-succing entity features hash

The datapack will contain two files: one with the per-entity counts and one with the entity to id mapping. Then, you can hash it using:

```bash
com.yahoo.semsearch.fastlinking.hash.QuasiSuccinctEntityHash \
  -i <datapack_file> -e <entity2id_file> -o <output_file>
```

## Models 

The following pre-trained models are provided to perform entity linking with the toolkit and are available through the Yahoo! webscope program for research purposes. These models are trained on Wikipedia and distributed using Creative Commons BY SA 4.0 license (see [MODELS_LICENSE](MODELS_LICENSE)).

### English 

* [English Hash trained from November 2015 Wikipedia](http://webscope.sandbox.yahoo.com/catalog.php?datatype=l&did=81)

* [English Entity Embeddings](http://webscope.sandbox.yahoo.com/catalog.php?datatype=l&did=81)

### Spanish

* [Spanish Hash trained from October 2015 Wikipedia](http://webscope.sandbox.yahoo.com/catalog.php?datatype=l&did=81)

* [Spanish Entity Embeddings](http://webscope.sandbox.yahoo.com/catalog.php?datatype=l&did=81)

### Chinese (Simplified)

* [Chinese Hash trained from December 2015 Wikipedia](http://webscope.sandbox.yahoo.com/catalog.php?datatype=l&did=81)

* [Chinese Entity Embeddings](http://webscope.sandbox.yahoo.com/catalog.php?datatype=l&did=81)

## Contact

[Roi Blanco](https://github.com/roicho), [Aasish Pappu](https://github.com/aasish)
