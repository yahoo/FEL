package com.yahoo.bcn.io;

import java.util.Collection;
import java.util.Set;



public interface DataReader {

	/**
	 * Provides a list of queries.
	 * 
	 * @return
	 */
	public Set<Query> getQueries();

	/**
	 * Provides a list of sessions.
	 * 
	 * @return
	 */
	public Collection<Session> getSessions();

	/**
	 * Provides the queries in a session.
	 * 
	 * @return
	 */
	public Set<Query> getQueriesForSession(Session s);

	/**
	 * Provides the query for an id.
	 * 
	 * @return
	 */
	public Query getQueryForId(String qid);

	/** 
	 * Saves the relevance assessments in TREC qrels style.
	 * 
	 * @param filename file to save to
	 */
	public void saveQrels(String filename);

	/** 
	 * Saves the queries and qIDs.
	 * 
	 * @param filename file to save to
	 */
	public void saveQueries(String filename);

	/** 
	 * Saves the sessions in BARSA format.
	 * 
	 * @param filename file to save to
	 */
	public void saveSessions(String filename);

}
