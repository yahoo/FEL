package com.yahoo.semsearch.fastlinking.view;

import java.util.ArrayList;

/**
 * Class that holds the features for the entity context. For a given
 * CandidateInfo (containing several entities) to calculate a score the same
 * EntityContext class will be used. This might be useful for caching, for
 * instance. The vanilla class implements an empty method (returns a score of 0
 * for every context).
 * Subclasses must at least implement the {@link #getEntityContextScorer( Entity e ) getEntityContextScorer method
 * 
 * @author roi 
 */
public abstract class EntityContext {
    public ArrayList<String> words;
    public void setEntitiesForScoring( Entity[] entities ){}

    /**
     * Call this method prior to scoring the context. This is useful to reuse as many calculations as possible in the context scorer (most likely many query 
     * segments will share contexts)
     * @param words
     */
    public void setContextWords( ArrayList<String> words ) {	
	if( words.size() == 0 ) return;
	ArrayList<String> filtered = new ArrayList<String>();
	filtered.add( words.get( 0 ) );
	for( int i = 1; i < words.size(); i++ ){
	    filtered.add( words.get( i ) );
	    filtered.add( words.get( i - 1 ) + "_" + words.get( i ) );
	}	
	this.words = filtered;
    }
  
    /**
     * Returns a score for the entity e using the words set previously using {@link #setContextWords( ArrayList<String> words) setContextWords} 
     * @param e
     * @return
     */
    public abstract double getEntityContextScore( Entity e );

    public String toString() {
	return "EntityContext";
    }

    /**
     * Provides a number to make scores comparable across queries. This normalizer is dependent on the method used to score entities.
     * @return
     */
    public float queryNormalizer(){
	return 1;
    }
}
