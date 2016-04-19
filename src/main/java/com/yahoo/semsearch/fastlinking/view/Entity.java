/**
 Copyright 2016, Yahoo Inc.
 Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 **/

package com.yahoo.semsearch.fastlinking.view;

import java.io.Serializable;

/**
 * This class stores the view representation of an entity. This contains its
 * features and identifier along with its type.
 * 
 * @author roi blanco
 *
 */

public class Entity implements Serializable {
    private static final long serialVersionUID = 1L;
    public int id;

    public Entity() {}

    public Entity( int id ) {
	this.id = id;
    }

    public short type; //Generic Freebase's type internal identifier.
    public double QEF; //The entity click frequency. The number of time any of entity's Wikipedia documents got clicked on US web search.
    public double QAEF; //The query-entity click frequency. The number of time any of entity's Wikipedia documents got clicked on US web search given the query.
    public double MET; //The entity click frequency. The number of time any of entity's Wikipedia documents got clicked on US web search. (WEC)
    public double MAET; //The matching alias-entity click frequency. The number of time any of entity's Wikipedia documents got clicked on US web search given the matching alias in the query. (WAEC)
    public double LET; //The entity link frequency. The number of time incoming links / anchor texts point to the entity in the Wikipedia corpus.
    public double LAET; //The link-entity frequency. The number of time the link / anchor text points to the entity in the Wikipedia corpus. (WANEC)

    @Override
    public int hashCode() {
	return id;
    }

    @Override
    public boolean equals( Object aThat ) {
	return ( (Entity) aThat ).id == id;
    }

    @Override
    public String toString() {
	return "id: " + id + " QEF: " + QEF + " QAEF: " + QAEF + " MET: " + MET + " MAET: " + MAET + " LET: " + LET + " LAET: " + LAET;
    }
}
