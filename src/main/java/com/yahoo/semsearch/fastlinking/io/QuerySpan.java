/**
 Copyright 2016, Yahoo Inc.
 Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 **/

package com.yahoo.semsearch.fastlinking.io;

/**
 * Simple class to hold a span annotation. 
 * 
 * @author emeij
 *
 */
public class QuerySpan {

	private String span = "";
	private String tag = "";
	private boolean main = false;

	private int startOffset = -1;
	private int endOffset = -1;

	private int relLevel = 1;

	public QuerySpan(String span, String tag, boolean main) {
		super();
		this.span = span;
		this.tag = tag;
		this.main = main;
	}

	public QuerySpan(String span, String tag, boolean main, int startOffset, int endOffset) {
		super();
		this.span = span;
		this.tag = tag;
		this.main = main;
		this.endOffset = endOffset;
		this.startOffset = startOffset;
	}

	public QuerySpan(String span, String tag, boolean main, int startOffset, int endOffset, int relLevel) {
		super();
		this.span = span;
		this.tag = tag;
		this.main = main;
		this.endOffset = endOffset;
		this.startOffset = startOffset;
		this.relLevel = relLevel;
	}

	public QuerySpan() {

	}

	public String getSpan() {
		return span;
	}

	public void setSpan(String span) {
		if (span.trim().length() > 1) {
			this.span = span;
		}
	}

	public String getTag() {
		return tag;
	}

	public void setTag(String tag) {
		if (!"not found".equalsIgnoreCase(tag)) {
			this.tag = tag;
		}
	}

	public boolean isMain() {
		return main;
	}

	public void setMain(boolean main) {
		this.main = main;
	}

	public int getStartOffset() {
		return startOffset;
	}

	public void setStartOffset(int startOffset) {
		this.startOffset = startOffset;
	}

	public int getEndOffset() {
		return endOffset;
	}

	public void setEndOffset(int endOffset) {
		this.endOffset = endOffset;
	}

	public int getRelLevel() {
		return relLevel;
	}

	public void setRelLevel(int relLevel) {
		this.relLevel = relLevel;
	}

	@Override
	public String toString() {
		return (main ? "(M) " : "( ) ") + span + " -> " + tag;
	}
}
