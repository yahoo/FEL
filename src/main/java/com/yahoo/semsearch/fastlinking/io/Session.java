/**
 Copyright 2016, Yahoo Inc.
 Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 **/

package com.yahoo.semsearch.fastlinking.io;

/**
 * Query session holder
 */
public class Session {

    private String user;

    private Query[] queries;

    public Session() {
        this.user = null;
    }

    public Session( String user ) {
        this.user = user;
    }

    public Query getQuery( int i ) {
        return queries[ i ];
    }

    public Query[] getQueries() {
        return queries;
    }

    public void setEvents( Query[] newEvents ) {
        queries = newEvents;
    }

    public String getUser() {
        return user;
    }

    public int getNumQueries() {
        return queries.length;
    }

}
