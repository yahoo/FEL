/**
 Copyright 2016, Yahoo Inc.
 Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 **/

package com.yahoo.semsearch.fastlinking.io;

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Simple class to hold a query annotation. 
 * 
 * @author emeij
 *
 */
public class Query implements  Comparable<Query>{

	public String ID;
	public String editor;
	public String query;
	public String location;

	public boolean cannot_judge = false;
	public boolean non_english = false;
	public boolean entity_no_wp = false;
	public boolean navigational = false;
	public boolean quote_or_question = false;
	public boolean adult = false;
	public boolean ambiguous = false;

	public Set<QuerySpan> spans = new LinkedHashSet<QuerySpan>();

	/// Bcookie of the session
	String bcookie;
	/// Timestamp of the session
	String timestamp;

	public Query(String ID, String editor, String query, boolean cannot_judge, boolean non_english,
			boolean entity_no_wp, boolean navigational, boolean quote_or_question, boolean adult, boolean ambiguous,
			Set<QuerySpan> spans) {
		super();
		this.ID = ID;
		this.editor = editor;
		this.query = query;
		this.cannot_judge = cannot_judge;
		this.non_english = non_english;
		this.entity_no_wp = entity_no_wp;
		this.navigational = navigational;
		this.quote_or_question = quote_or_question;
		this.adult = adult;
		this.ambiguous = ambiguous;
		this.spans = spans;
	}

	public Query(String ID, String editor, String query, String location, boolean cannot_judge, boolean non_english,
			boolean entity_no_wp, boolean navigational, boolean quote_or_question, boolean adult, boolean ambiguous,
			Set<QuerySpan> spans) {
		super();
		this.ID = ID;
		this.editor = editor;
		this.query = query;
		this.location = location;
		this.cannot_judge = cannot_judge;
		this.non_english = non_english;
		this.entity_no_wp = entity_no_wp;
		this.navigational = navigational;
		this.quote_or_question = quote_or_question;
		this.adult = adult;
		this.ambiguous = ambiguous;
		this.spans = spans;
	}

	public Query() {

	}

	public String getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(String timestamp) {
		this.timestamp = timestamp;
	}

	public String getBcookie() {
		return bcookie;
	}

	public void setBcookie(String bcookie) {
		this.bcookie = bcookie.startsWith("B::") ? bcookie.substring(3) : bcookie;
	}

	public String getEditor() {
		return editor;
	}

	public void setEditor(String editor) {
		this.editor = editor;
	}

	public String getQuery() {
		return query;
	}

	public void setQuery(String query) {
		this.query = query;
	}

	public String getLocation() {
		return location;
	}

	public void setLocation(String location) {
		this.location = location;
	}

	public boolean isCannot_judge() {
		return cannot_judge;
	}

	public void setCannot_judge(boolean cannot_judge) {
		this.cannot_judge = cannot_judge;
	}

	public boolean isNon_english() {
		return non_english;
	}

	public void setNon_english(boolean non_english) {
		this.non_english = non_english;
	}

	public boolean isEntity_no_wp() {
		return entity_no_wp;
	}

	public void setEntity_no_wp(boolean entity_no_wp) {
		this.entity_no_wp = entity_no_wp;
	}

	public boolean isNavigational() {
		return navigational;
	}

	public void setNavigational(boolean navigational) {
		this.navigational = navigational;
	}

	public boolean isQuote_or_question() {
		return quote_or_question;
	}

	public void setQuote_or_question(boolean quote_or_question) {
		this.quote_or_question = quote_or_question;
	}

	public boolean isAdult() {
		return adult;
	}

	public void setAdult(boolean adult) {
		this.adult = adult;
	}

	public boolean isAmbiguous() {
		return ambiguous;
	}

	public void setAmbiguous(boolean ambiguous) {
		this.ambiguous = ambiguous;
	}

	public Set<QuerySpan> getSpans() {
		return spans;
	}

	public void setSpans(Set<QuerySpan> spans) {
		this.spans = spans;
	}

	public String getID() {
		return ID;
	}

	public void setID(String iD) {
		ID = iD;
	}

	@Override
	public String toString() {
		return "Query [ID=" + ID + ", editor=" + editor + ", query=" + query + ", cannot_judge=" + cannot_judge
				+ ", non_english=" + non_english + ", entity_no_wp=" + entity_no_wp + ", navigational=" + navigational
				+ ", quote_or_question=" + quote_or_question + ", adult=" + adult + ", ambiguous=" + ambiguous
				+ ", spans=" + spans.toString() + "]";

	}

	//@Override
	public int compareTo(Query o) {
		return Integer.valueOf(ID).compareTo(Integer.valueOf(o.getID()));
	}

	@Override
	public int hashCode() {
		return ID.hashCode() * 31 + bcookie.hashCode() * 17 + location.hashCode() * 5 + query.hashCode() * 3;
	}

	@Override
	public boolean equals(Object other) {

		if (!(other instanceof Query))
			return false;

		Query otherQuery = (Query) other;
		return ID.equals(otherQuery.ID) && query.equals(otherQuery.query) && location.equals(otherQuery.location)
				&& bcookie.equals(otherQuery.bcookie);

	}

}
