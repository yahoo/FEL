/**
 Copyright 2016, Yahoo Inc.
 Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 **/

package com.yahoo.semsearch.fastlinking.w2v;

import java.io.IOException;
import java.util.Hashtable;

import helma.xmlrpc.*;

/**
 * Created by aasishkp on 2/4/16.
 *
 *
 *  java -Xmx512m -Xmx10g exec:java -Dexec.mainClass=com.yahoo.semsearch.fastlinking.w2v.EmbeddingsServer -Dexec.args=enwiki.wiki2vec.d300.compressed  -Dexec.classpathScope=compile

 */
public class EmbeddingsServer {


        private CompressedSingleFileEntitySimilarityUtil util ;
        public EmbeddingsServer(String wordEmbeddings) {
            // Our handler is a regular Java object. It can have a
            // constructor and member variables in the ordinary fashion.
            // Public methods will be exposed to XML-RPC clients.
            System.out.println(wordEmbeddings);
            try {
                    util = new CompressedSingleFileEntitySimilarityUtil(wordEmbeddings);

                String[] phrases = new String[]{"the life of brian", "this is a test phrase"};
                String[] entities = new String[]{"_wiki_Monty_Python", "_wiki_John_Cleese", "_WIKI_Brad_Pitt"};
                System.out.println(util.entity2EntitySimilarity(entities[0], entities[1]));
            }
            catch(IOException e){

            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
        }

        public Hashtable getVector(String entity){
            Hashtable result = new Hashtable();
            float[] vector = util.getVector(entity);
            int dimCount = 0;
            for(float dim:vector) {
                result.put(String.valueOf(dimCount), dim);
                dimCount++;
            }
            return result;

        }
        public Hashtable entity2EntitySimilarity(String entity1, String entity2){
            Hashtable result = new Hashtable();
            double similarityValue =   util.entity2EntitySimilarity(entity1, entity2);
            result.put("similarity", new Double(similarityValue));
            return result;
            //return util.entity2EntitySimilarity(entity1, entity2);

        }

        public Hashtable entity2WordSimilarity( String entity, String phrase ) {
            Hashtable result = new Hashtable();
            double similarityValue =   util.entity2WordSimilarity(entity, phrase);
            result.put("similarity", new Double(similarityValue));
            return result;
        }

        public Hashtable sumAndDifference (int x, int y) {
            Hashtable result = new Hashtable();
            result.put("sum", new Integer(x + y));
            result.put("difference", new Integer(x - y));
            return result;
        }


        public static void main (String [] args) {
            try {

                System.out.println("JavaServer: started");
                WebServer server = new WebServer(8999);

                server.addHandler("sample", new EmbeddingsServer(args[0]));


            } catch (Exception exception) {
                System.err.println("JavaServer: " + exception.toString());
            }
        }


}
