/**
 Copyright 2016, Yahoo Inc.
 Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 **/

package com.yahoo.semsearch.fastlinking.w2v;

import java.io.IOException;

/**
 * Created by roi on 1/5/16.
 */
public class CompressedEntitySimilarityUtil {

    private CompressedW2V words;
    private CompressedW2V entities;
    private int dimensions;

    public CompressedEntitySimilarityUtil(){}
    /**
     * @param wordsVectors
     * * @param entityVectors
     * @throws IOException
     */
    public CompressedEntitySimilarityUtil( String wordsVectors, String entityVectors ) throws IOException, ClassNotFoundException {
        words = new CompressedW2V( wordsVectors );
        entities = new CompressedW2V( entityVectors );
        dimensions = words.N;
    }

    /**
     *
     * @param entity1
     * @param entity2
     * @return word2vec cosine similarity
     */
    public double entity2EntitySimilarity(String entity1, String entity2) {
        float[] e1 = entities.getVectorOf( processEntityId( entity1 ) );
        float[] e2 = entities.getVectorOf( processEntityId( entity2 ) );
        if( e1 != null && e2 != null ) {
            return WordVectorsUtils.sim(e1, e2, dimensions);
        }
        return 0D;
    }

    /**
     *
     * @param entity
     * @param phrase
     * @return word2vec cosine similarity
     */

    public double entity2WordSimilarity(String entity, String phrase ) {
        float[] entityVector = entities.getVectorOf( processEntityId( entity ) );
        if( entityVector == null ) return 0D;
        String[] parts = phrase.split( "\\s+" );
        double s = 0D;
        for( int i = 1; i < parts.length; i++ ){
            float[] word = words.getVectorOf( parts[ i ] );
            if( word != null ) s += Math.log( 1 + Math.exp( WordVectorsUtils.sim(entityVector, word, dimensions) ) );
            word = words.getVectorOf( parts[ i - 1 ] +"_" + parts[ i ] );
            if( word != null ) s += Math.log( 1 + Math.exp( WordVectorsUtils.sim(entityVector, word, dimensions) ) );
        }
        return -s;
    }

    final int prepLength = "_wiki_".length();
    //Horrible (temporary) hack to remove the prepended wiki tag
    public String processEntityId( String entityID ){
        return entityID.substring( prepLength );
    }


    public static void main ( String args[] ) throws IOException, ClassNotFoundException {
        CompressedEntitySimilarityUtil util = new CompressedEntitySimilarityUtil( args[ 0 ], args[ 1 ] );
        String[] phrases = new String[]{ "the life of brian", "this is a test phrase" };
        String[] entities = new String[]{ "_wiki_Monty_Python", "_wiki_John_Cleese", "_wiki_Brad_Pitt"};
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
