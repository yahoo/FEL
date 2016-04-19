/**
 Copyright 2016, Yahoo Inc.
 Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 **/

package com.yahoo.semsearch.fastlinking.view;

import java.util.ArrayList;

/**
 * Entity context that always returns zero as the contex score 
 * @author roi blanco
 *
 */
public class EmptyContext extends EntityContext {
    @Override
    public void setContextWords( ArrayList<String> words ){}

    @Override
    public double getEntityContextScore( Entity e ) {
	return 0;	
    }
    
    @Override
    public void setEntitiesForScoring( Entity[] entities ) {}
}
