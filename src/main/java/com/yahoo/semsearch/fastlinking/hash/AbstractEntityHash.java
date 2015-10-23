package com.yahoo.semsearch.fastlinking.hash;

import it.unimi.dsi.fastutil.objects.Object2LongFunction;

import java.io.Serializable;
import java.util.regex.Pattern;

import com.yahoo.semsearch.fastlinking.view.CandidatesInfo;

/**
 * Super-class for holding hashes that store aliases and features for those aliases.
 * Sub-classes must extend two methods
 *  - a getCandidatesInfo method for getting a CandidatesInfo object out of a String surface form, this is
 * 		given a string representing an alias, return all the stored features
 *  - a getEntityName method that returns the string name identifier of an entity given its integer id 
 * @author roi
 *
 */
public abstract class AbstractEntityHash implements Serializable {

    private static final long serialVersionUID = 1L;
    final Object2LongFunction<? extends CharSequence> hash;
    private static final Pattern WHITESPACE = Pattern.compile( "\\s+" );
    private static final Pattern PUNCTUATION = Pattern.compile( "\\p{Punct}" );

    AbstractEntityHash( final Object2LongFunction<? extends CharSequence> hash ) {
	this.hash = hash;
    }

    /**
     * Extract a set of stored features in the CandidatesInfo object out of a surfaceForm alias 
     * @param surfaceForm
     * @return
     */
    public abstract CandidatesInfo getCandidatesInfo( String surfaceForm );

    /**
     * return the string identifier of an entity given its integer identifier
     * @param id
     * @return
     */
    public abstract CharSequence getEntityName( int id );

 

}