/**
 Copyright 2016, Yahoo Inc.
 Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 **/

package com.yahoo.semsearch.fastlinking.view;

import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.yahoo.semsearch.fastlinking.EntityContextFastEntityLinker;
import com.yahoo.semsearch.fastlinking.hash.AbstractEntityHash;
import com.yahoo.semsearch.fastlinking.utils.TypeLanguageModel;
import com.yahoo.semsearch.fastlinking.w2v.LREntityContext;

/**
 * This class implements a context that assigns a score using the type of the
 * entity and the likelihood that the context words belong to this particular
 * type. The language model must have been calculated previously using @see
 * com.yahoo.semsearch.fastlinking.utils.TypeLanguageModel
 * Internally the class uses a cache for type scoring that is flushed out each time the context words are set using @see setContextWords
 *
 * @author roi blanco
 */
public class LMLREntityContext extends LREntityContext {
    private TypeLanguageModel models;
    private HashMap<Short, String> typeMapping;
    private HashMap<Short, Double> scoreCache;
    private List<String> ngrams;
    private static final int muLM = 1000;
    private static final double DEFAULT_SCORE = -50;

    public LMLREntityContext( String wordsFile, String entityF, AbstractEntityHash hash, String modelFile, String typeMappingFile ) throws ClassNotFoundException, IOException {
        super( wordsFile, entityF, hash );
        typeMapping = EntityContextFastEntityLinker.readTypeMapping( typeMappingFile );
        models = ( TypeLanguageModel ) BinIO.loadObject( modelFile );
        reset();
    }

    @Override
    public void setContextWords( ArrayList<String> words ) {
        super.setContextWords( words );
        ngrams = TypeLanguageModel.getTrigrams( words );
        reset();
    }

    /**
     * Flushes out the scoring cache
     */
    private void reset() {
        scoreCache = new HashMap<Short, Double>();
    }

    /**
     * Computes the score of a given type using the log-likelihood with respect to a previously computed language model
     *
     * @param type identifier of the type you are scoring
     * @return score of the type for the previously set context
     */
    public double getScoreOf( short type ) {
        Double score = scoreCache.get( type );
        if( score != null ) return score;
        score = 0D;
        final String t = typeMapping.get( type );
        if( t == null ) return DEFAULT_SCORE; //type not found in mapping - warning?
        final Object2IntOpenHashMap<String> lm = models.languageModels.get( t );
        if( lm == null ) return DEFAULT_SCORE;
        for( String w : ngrams ) {
            Integer f = lm.get( w );
            if( f != null ) { //else add zero, = do nothing
                score += Math.log( ( f + muLM * ( ( double ) models.backgroundModel.get( w ) / models.totalFreq ) ) / ( models.freqs.get( t ) + muLM ) );
            }
        }
        if( score == 0D ) score = DEFAULT_SCORE;
        scoreCache.put( type, score );
        return score;
    }
}
