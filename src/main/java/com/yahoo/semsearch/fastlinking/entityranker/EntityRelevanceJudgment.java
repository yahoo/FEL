/**
 Copyright 2016, Yahoo Inc.
 Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 **/

package com.yahoo.semsearch.fastlinking.entityranker;

import java.net.URLEncoder;

/**
 * Class holder for relevance judgments.
 * Contains tuples of the form
 * (query, label, location, is relevant?)
 *
 * @author roi blanco
 */
public class EntityRelevanceJudgment {
    public String id;
    public String label;
    public boolean relevant;
    public String location;

    public EntityRelevanceJudgment( String id, String label, boolean rel, String location ) {
        //this.id = id;
        try {
            this.id = URLEncoder.encode( id.toString(), "UTF-8" );
        } catch( Exception e ) {
            System.err.println( "Can't encode " + this.id );
        }
        this.label = label;
        this.relevant = rel;
        this.location = location;
    }

    public void setLabel( String l ) {
        this.label = l;
    }

    public void setid( String id ) {
        this.id = id;
    }
}