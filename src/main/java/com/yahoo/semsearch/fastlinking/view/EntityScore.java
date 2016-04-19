/**
 Copyright 2016, Yahoo Inc.
 Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 **/

package com.yahoo.semsearch.fastlinking.view;

import java.util.HashMap;

/**
 * Class for holding a score of a given entity. It also contains a HashMap for holding feature values.
 * @author roi blanco
 *
 */
public class EntityScore implements Comparable<EntityScore>{
 	public HashMap<String, Double> features;
 	public Entity entity;
	public double score;
	
 	public EntityScore( Entity e, double d ) {
	    this.entity = e;
	    this.score = d;
	}
	
	@Override
	public int compareTo( EntityScore o ) {	    
	    return  -Double.compare(score, o.score );
	}
  }