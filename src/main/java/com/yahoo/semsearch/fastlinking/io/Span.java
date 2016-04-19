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
public class Span {

	private String span = "";
	private String tag = "";
	private boolean main = false;

	public Span(String span, String tag, boolean main) {
		super();
		this.span = span;
		this.tag = tag;
		this.main = main;
	}

	public Span() {

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

	@Override
	public String toString() {
		return (main ? "(M) " : "( ) ") + span + " -> " + tag;
	}
}
