/**
 Copyright 2016, Yahoo Inc.
 Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 **/

package com.yahoo.semsearch.fastlinking.hash;


import it.unimi.dsi.fastutil.objects.Object2LongFunction;
import it.unimi.dsi.fastutil.objects.ObjectBigArrayBigList;

import java.io.Serializable;

import com.yahoo.semsearch.fastlinking.view.CandidatesInfo;

/**
 * Hash class that stores uncompressed features in an array
 *
 * @author roi blanco
 */
public class EntityHash extends AbstractEntityHash implements Serializable {

    private static final long serialVersionUID = 1L;
    public CandidatesInfo[] infos;
    private ObjectBigArrayBigList<String> names;

    public EntityHash( Object2LongFunction<? extends CharSequence> hash, CandidatesInfo[] infos, ObjectBigArrayBigList<String> names ) {
        super( hash );
        this.infos = infos;
        this.names = names;
    }

    @Override
    public CandidatesInfo getCandidatesInfo( String surfaceForm ) {
        long id = hash.getLong( surfaceForm );
        if( id != -1 ) {
            assert id < infos.length;
            return infos[ ( int ) id ];
        }
        return null;
    }

    @Override
    public CharSequence getEntityName( int id ) {
        return names.get( id );
    }

}
