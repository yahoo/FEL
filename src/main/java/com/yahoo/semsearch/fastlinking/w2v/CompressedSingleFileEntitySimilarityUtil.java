/**
 Copyright 2016, Yahoo Inc.
 Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 **/
package com.yahoo.semsearch.fastlinking.w2v;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * Loads entities and word embeddings that have been compressed on the same name
 * Created by roi on 2/3/16.
 */
public class CompressedSingleFileEntitySimilarityUtil extends CompressedEntitySimilarityUtil {
    private CompressedW2V vector;
    private int dimensions;

    public CompressedSingleFileEntitySimilarityUtil(){}
    /**
     * @param embeddings file with the word embeddings location
     * @throws IOException
     */
    public CompressedSingleFileEntitySimilarityUtil( String embeddings ) throws IOException, ClassNotFoundException {
        vector = new CompressedW2V( embeddings );
        dimensions = vector.N;
    }

    public float[] getVector(String phrase){
        return vector.getVectorOf(phrase);
    }
    /**
     *
     * @param entity1 string representing an entity
     * @param entity2 string representing an entity
     * @return word2vec cosine similarity
     */
    @Override
    public double entity2EntitySimilarity(String entity1, String entity2) {
        float[] e1 = vector.getVectorOf( entity1  );
        float[] e2 = vector.getVectorOf( entity2  );
        if( e1 != null && e2 != null ) {
            return WordVectorsUtils.sim(e1, e2, dimensions);
        }
        return 0D;
    }


    /**
     *
     * @param entity string representing an entity
     * @param phrase string representing a phrase (will be split using spaces)
     * @return Average cosine similarity
     */
    @Override
    public double entity2WordSimilarity( String entity, String phrase ) {
        float[] entityVector = vector.getVectorOf( entity );
        if( entityVector == null ) return 0D;
        String[] parts = phrase.split( "\\s+" );
        double s = 0D;
        int numberOfWords = 0;
        for( int i = 0; i < parts.length; i++ ){
            float[] word = vector.getVectorOf( parts[ i ] );
            if( word != null ) {
                s += WordVectorsUtils.sim(entityVector, word, dimensions);
                numberOfWords++;
            }
            if(i > 0){
                word = vector.getVectorOf(parts[i - 1] + "_" + parts[i]);
                if (word != null) {
                    s += WordVectorsUtils.sim(entityVector, word, dimensions);
                    numberOfWords++;
                }
            }
        }
        if( numberOfWords == 0 ) return 0;
        return s/numberOfWords;
    }

    /**
     *
     * @param entity string representing an entity
     * @param phrase string representing a phrase (will be split using spaces)
     * @return Cosine similariy of the entity and the centroid of the phrase
     */
    public double entity2WordSimilarityCentroid( String entity, String phrase ) {
        float[] entityVector = vector.getVectorOf( processEntityId( entity ) );
        if( entityVector == null ) return 0D;
        String[] parts = phrase.split( "\\s+" );
        double s = 0D;
        float[] centroid = new float[ dimensions ];
        int numberOfWords = 0;
        for( int i = 1; i < parts.length; i++ ){
            float[] word = vector.getVectorOf( parts[ i ] );
            if( word != null ) {
                for( int j = 0; i < dimensions; j++ ) centroid[ j ] += word[ j ];
                numberOfWords++;
            }
            word = vector.getVectorOf( parts[ i - 1 ] +"_" + parts[ i ] );
            if( word != null ){
                for( int j = 0; i < dimensions; j++ ) centroid[ j ] += word[ j ];
                numberOfWords++;
            }
        }
        if( numberOfWords == 0 ) return 0;
        for( int i = 0; i < dimensions; i++ ){
            centroid[ i ] /= numberOfWords;
        }
        return WordVectorsUtils.sim(entityVector, centroid, dimensions);
    }

    /** @param str
     *  @return an array of adjacent letter pairs contained in the input string */
    private static String[] letterPairs(String str) {
        if (str.length() < 2) {
            return new String[0];
        }
        int numPairs = str.length()-1;
        String[] pairs = new String[numPairs];
        for (int i=0; i<numPairs; i++) {
            pairs[i] = str.substring(i,i+2);
        }
        return pairs;
    }

    /**  @param str
     *  @return an ArrayList of 2-character Strings. */
    private static List<String> wordLetterPairs(String str) {
        List<String> allPairs = new ArrayList<String>();
        // Tokenize the string and put the tokens/words into an array
        String[] words = str.split("\\s");
        // For each word
        for (int w=0; w < words.length; w++) {
            // Find the pairs of characters
            String[] pairsInWord = letterPairs(words[w]);
            for (int p=0; p < pairsInWord.length; p++) {
                allPairs.add(pairsInWord[p]);
            }
        }
        return allPairs;
    }

    /** @param phrase1
     * @param phrase2
     * @return lexical similarity value in the range [0,1] */
    public static double lexicalSimilarity(String phrase1, String phrase2) {
        List<String> pairs1 = wordLetterPairs(phrase1.toUpperCase());
        List<String> pairs2 = wordLetterPairs(phrase2.toUpperCase());
        int intersection = 0;
        int union = pairs1.size() + pairs2.size();
        for (int i=0; i<pairs1.size(); i++) {
            String pair1 = pairs1.get(i);
            for(int j=0; j<pairs2.size(); j++) {
                String pair2 = pairs2.get(j);
                if (pair1.equals(pair2)) {
                    intersection++;
                    pairs2.remove(j);
                    break;
                }
            }
        }
        return (2.0*intersection)/union;
    }


    public static void main ( String args[] ) throws IOException, ClassNotFoundException {
        CompressedSingleFileEntitySimilarityUtil util = new CompressedSingleFileEntitySimilarityUtil(args[0]);
//        CompressedEntitySimilarityUtil util = new CompressedEntitySimilarityUtil( args[ 0 ], args[ 1 ] );
        String[] phrases = new String[]{ "the life of brian", "this is a test phrase" , "Dubai" };
        String[] entities = new String[]{ "_wiki_Monty_Python", "_wiki_John_Cleese", "_wiki_Brad_Pitt" , "_wiki_United_Arab_Emirates"};
        String[] chineseEntities = new String[] {"_wiki_孟二冬", "_wiki_奧地利國旗"};
        for( String p : phrases )
            for( String e : entities ){
                System.out.println( "s( " + e + "," + p +" ) = " + util.entity2WordSimilarity( e, p ));
            }
        for( String e : entities )
            for( String e2 : entities ){
                System.out.println( "s( " + e + "," + e2 +" ) = " + util.entity2EntitySimilarity( e, e2 ));
            }

    }


}
