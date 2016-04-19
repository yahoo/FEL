/**
 Copyright 2016, Yahoo Inc.
 Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 **/

package com.yahoo.semsearch.fastlinking.w2v;


import java.util.ArrayList;

import com.yahoo.semsearch.fastlinking.hash.AbstractEntityHash;
import com.yahoo.semsearch.fastlinking.view.Entity;
import com.yahoo.semsearch.fastlinking.view.EntityContext;

/**
 * Computes the context score using a vector for the entities
 *
 * @author roi blanco
 */
public class QueryEntityVectorContext extends EntityContext {
    float[] centroid; //acc the centroid of the words in the context
    ArrayList<String> words;//remove
    final int N;
    final protected WordVectors unigramV;
    final protected WordVectors entityV;
    final protected AbstractEntityHash hash;
    boolean DEBUG = false;


    final protected ArrayList<String> sw = new ArrayList<String>() {
        private static final long serialVersionUID = 1L;

        {
            add( "of" );
            add( "the" );
            add( "in" );
            add( "at" );
            add( "on" );
            add( "for" );
        }
    };


    public QueryEntityVectorContext( WordVectors unigramV, WordVectors entityV, AbstractEntityHash hash ) {
        this.unigramV = unigramV;
        this.entityV = entityV;
        this.hash = hash;
        this.N = unigramV.getVectorLength();
    }

    @Override
    public void setContextWords( ArrayList<String> words ) {
        words.removeAll( sw );
        centroid = WordVectorsUtils.centroid( words, N, unigramV );
        this.words = words;
    }

    @Override
    public double getEntityContextScore( Entity e ) {
        CharSequence name = hash.getEntityName( e.id );
        float[] vector = entityV.getVectorOf( name.toString() );
        if( vector != null ) {
            double distance = WordVectorsUtils.sim( vector, centroid, N );
            if( DEBUG ) System.out.println( " Sim of <" + name + "> to " + words + " is = " + distance );
            return distance;
        } else {
            if( DEBUG ) System.out.println( " <" + name + "> doesn't have a vector" );
        }
        return 0;
    }

    @Override
    public String toString() {
        return "QueryEntityVectorContext";
    }

}
