test
=======

### Fiddling with word embeddings

This package also provides code to
  1. quantize word2vec vectors for uni/bigrams
  2. compress quantized vectors
  3. generate word vectors for entities, given a set of words that describe them. In this context n entity is anything that has both an identifier and a sequence of words describing it - a document, sentence could match this definition.


#### Word vectors

First, compute word embeddings using whatever software you prefer, and output the word vectors in word2vec C format.

To quantize the vectors:

```bash
java com.yahoo.semsearch.fastlinking.w2v.Quantizer -i <word_embeddings> -o <output> -h
```

The program will try to find the optimum quantization factor that is below a pre-specified error loss (default 0.01) using binary search.
However, if you have a large number (likely) of vectors, it might take a while to find the right quantization factor, and you might want to use one straight ahead:

```bash
java com.yahoo.semsearch.fastlinking.w2v.Quantizer -i <embeddings> -o <output> -d 8
```

The -h flag should be used when the vectors file contains a header, like in the case of the c implemantation of word2vec - it will simply skip it.

To compress the vectors:

```bash
java -Xmx5G it.cnr.isti.hpd.Word2VecCompress <quantized_file> <output>
```

If you run out of memory using the above class, you can use the following class (it scales up to millions of vectors):

```bash
com.yahoo.semsearch.fastlinking.w2v.EfficientWord2VecCompress
```

#### Entity vectors

There are many ways to generate entity vectors. Here we describe a process that takes as the entity representation the first paragraph of the entity's corresponding Wikipedia page. If you have other way of representing the entities (more or other kind of text) then this could be added without any hassle.
The program can run on hadoop as well as in standalone mode.

The steps goes as follows:
1. Download the wiki dump and unzip it ([see the io package](src/main/java/com/yahoo/semsearch/fastlinking/io/README.md))
2. Extract the first paragrahps for every entity out of the unzipped dump

```bash
java -Dfile.encoding=UTF-8 com.yahoo.semsearch.fastlinking.utils.ExtractFirstParagraphs <input_wiki_dump> <output_file>
```

3. (HADOOP only) Split the paragraphs into different files. This helps the balance the load without a proper hdfs file splitter.

```bash
split -b10M --suffix-length=6 <paragraph_file>
```

4. Copy from local to hdfs

```bash
hadoop fs -mkdir E2W;
hadoop fs -copyFromLocal x* E2W
```

5. Create the embeddings. The program accepts a file with the following format (one per line)

```
entity_id <TAB> number <TAB> word sequence
```

Assuming your word vectors file is called word_vectors (HADOOP)

```bash
hadoop jar FEL-0.1.0-fat.jar com.yahoo.semsearch.fastlinking.w2v.EntityEmbeddings  -Dmapreduce.job.queuename=adhoc -files word_vectors#vectors E2W entity.embeddings
```

6. ( HADOOP only) Collect the data
Then, you can quantize and compress the resulting vectors, just like we did for word embeddings.

```bash
hadoop fs -copyToLocal entity.embeddings/part-r-00000 entity.embeddings
```

7. Quantize the vectors

```bash
java -Xmx2G -cp .:FEL-0.1.0-fat.jar com.yahoo.semsearch.fastlinking.w2v.Quantizer -i entity.embeddings -o entity.embeddings.quant -d -q 9
```

8. Compress the vectors

```bash
java -cp FEL-0.1.0-fat.jar com.yahoo.semsearch.fastlinking.w2v.EfficientWord2VecCompress entity.embeddings.quant entity.embeddings.compress
```
