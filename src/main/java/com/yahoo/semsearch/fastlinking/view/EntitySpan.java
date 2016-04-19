/**
 Copyright 2016, Yahoo Inc.
 Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 **/

package com.yahoo.semsearch.fastlinking.view;


/**
 * Extended class to hold the entity annotations.
 * 
 * @author emeij
 *
 */
public class EntitySpan extends Span {

    public Entity e; //holds the entity for this Span, if found
    public double score; //holds the score for this Span if found
    private String entity;

    public EntitySpan( String span ) {
	super( span );
    }

    public EntitySpan( String span, String entity, int startOffset, int endOffset ) {
	super( span, startOffset, endOffset );
	this.entity = entity;
    }

    public EntitySpan( Span mspan, Entity e, double score ) {
	super( mspan );
	this.e = e;
	this.score = score;
    }

    public String getEntity() {
	return entity;
    }

    public void setEntity( String entity ) {
	this.entity = entity;
    }

    public double getScore() {
	return score;
    }

    public void setScore( double score ) {
	this.score = score;
    }
}
