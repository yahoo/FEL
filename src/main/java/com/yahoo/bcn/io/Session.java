package com.yahoo.bcn.io;

import org.apache.log4j.Logger;

public class Session {

	public static final Logger logger = Logger.getLogger(Session.class);

	private String user;

	private Query[] queries;

	public Session() {
		this.user = null;
	}

	public Session(String user) {
		this.user = user;
	}

	public Query getQuery(int i) {
		return queries[i];
	}

	public Query[] getQueries() {
		return queries;
	}

	public void setEvents(Query[] newEvents) {
		queries = newEvents;
	}

	public String getUser() {
		return user;
	}

	public int getNumQueries() {
		return queries.length;
	}

}
