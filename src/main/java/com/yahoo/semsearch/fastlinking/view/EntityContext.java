/**
 Copyright 2016, Yahoo Inc.
 Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 **/

package com.yahoo.semsearch.fastlinking.view;

import java.util.ArrayList;

/**
 * Class that holds the features for the entity context. For a given
 * CandidateInfo (containing several entities) to calculate a score the same
 * EntityContext class will be used. This might be useful for caching, for
 * instance. The vanilla class implements an empty method (returns a score of 0
 * for every context).
 * Subclasses must at least implement the {@link #getEntityContextScore(Entity ) getEntityContextScorer method
 *
 * @author roi blanco
 */
public abstract class EntityContext {
    public ArrayList<String> words;

    public void setEntitiesForScoring( Entity[] entities ) {}

    /**
     * Call this method prior to scoring the context. This is useful to reuse as many calculations as possible in the context scorer (most likely many query
     * segments will share contexts)
     *
     * @param words list of strings to be used as the context input
     */
    public void setContextWords( ArrayList<String> words ) {
        if( words.size() == 0 ) return;
        ArrayList<String> filtered = new ArrayList<String>();
        filtered.add( words.get( 0 ) );
        for( int i = 1; i < words.size(); i++ ) {
            filtered.add( words.get( i ) );
            filtered.add( words.get( i - 1 ) + "_" + words.get( i ) );
        }
        this.words = filtered;
    }

    /**
     * Returns a score for the entity e using the words set previously using {@link #setContextWords(ArrayList<String> ) setContextWords}
     * @param e entity to be scored
     * @return context score for e (context has to be previously set)
     */
    public abstract double getEntityContextScore( Entity e );

    public String toString() {
        return "EntityContext";
    }

    /**
     * Provides a number to make scores comparable across queries. This normalizer is dependent on the method used to score entities.
     *
     * @return the value to normalize the per query score to
     */
    public float queryNormalizer() {
        return 1;
    }
}
