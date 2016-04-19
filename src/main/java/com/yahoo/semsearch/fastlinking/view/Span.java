/**
 Copyright 2016, Yahoo Inc.
 Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 **/

package com.yahoo.semsearch.fastlinking.view;

import java.util.Comparator;

/**
 * Simple class to hold a span annotation.
 * 
 * @author emeij
 *
 */
public class Span implements Comparable<Span>, Comparator<Span> {

    private String query;
    public String span;
    private Integer startOffset;
    private Integer endOffset;

    Span( String span ) {
	this.span = span;
    }

    Span( Span span ) {
	this.span = span.span;
	this.startOffset = span.startOffset;
	this.endOffset = span.endOffset;
    }

    public Span( String span, int startOffset, int endOffset ) {
	this.span = span;
	this.startOffset = startOffset;
	this.endOffset = endOffset;
    }

    Span( String span, int startOffset, int endOffset, String query ) {
	this.span = span;
	this.startOffset = startOffset;
	this.endOffset = endOffset;
	this.query = query;
    }

    /**
     * @return the span in the input that matched.
     */
    public String getSpan() {
	return span;
    }

    public void setSpan( String span ) {
	this.span = span;
    }

    public String getQuery() {
	return query;
    }

    public void setQuery( String query ) {
	this.query = query;
    }

    public int getStartOffset() {
	return startOffset;
    }

    public void setStartOffset( int startOffset ) {
	this.startOffset = startOffset;
    }

    public int getEndOffset() {
	return endOffset;
    }

    public void setEndOffset( int endOffset ) {
	this.endOffset = endOffset;
    }

    /**
     * Only checks whether the actual span (the String) is the same.
     * 
     */
    @Override
    public boolean equals( Object other ) {
	return ( other == null || other.getClass() != getClass() ) ? false : ( (Span) other ).getSpan().equals( getSpan() );
    }

    /**
     * Only checks whether the actual span (the String) is the same.
     * 
     */
    @Override
    public int compare( Span s1, Span s2 ) {
	return s1.getSpan().compareTo( s2.getSpan() );
    }

    /**
     * Only checks whether the actual span (the String) is the same.
     * 
     */
    @Override
    public int compareTo( Span s ) {
	return s.getSpan().compareTo( getSpan() );
    }

    /**
     * Only uses the actual span (the String).
     * 
     */
    @Override
    public int hashCode() {
	return getSpan().hashCode();
    }

    public String toString() {
	return span;
    }
}
