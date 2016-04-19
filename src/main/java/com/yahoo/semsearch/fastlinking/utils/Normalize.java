/**
 Copyright 2016, Yahoo Inc.
 Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 **/

package com.yahoo.semsearch.fastlinking.utils;

import java.text.Normalizer;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;

import com.yahoo.semsearch.fastlinking.view.Span;

/**
 * Factory for normalizing strings of text.
 * It implements several methods and wraps one of them in the {@link #normalize(String args[]) normalize method.
 *
 * @author roi blanco
 */
public class Normalize {
    private static final Pattern SPACE = Pattern.compile( "[\\p{Space}\\p{Cntrl}]+" );
    private static final Pattern PUNCT = Pattern.compile( "\\p{Punct}+" );
    private static final String SUBST_EMPTY = Matcher.quoteReplacement( "" );
    private static final String SUBST_SPACE = Matcher.quoteReplacement( " " );
    private static final Pattern DIACRITICS = Pattern.compile( "[\\p{InCombiningDiacriticalMarks}\\p{IsLm}\\p{IsSk}]+" );
    private static final String[] ID_SW = new String[]{ " the ", " of ", " a ", " at ", " in " };

    /**
     * Removes non digit or letter characters from the input string.
     * This method is a wrapper for one of the different normalize methods implemented in this class.
     *
     * @param args string to normalize
     * @return processed string
     */
    public static String normalize( String args ) {
        return normalizeFast( args );
    }

    /**
     * Normalizes and returns a span array
     * @param args string to normalize
     * @return processed span array with (normalized) string chunks
     */
    public static Span[] normalizeWithSpans( String args ) {
        final StringBuilder t = new StringBuilder();
        final int length = args.length();
        List<Span> res = new ArrayList<Span>();
        int pi = -1;
        for( int i = 0; i < length; i++ ) {
            char charAt = args.charAt( i );
            if( Character.isLetterOrDigit( charAt ) && !Character.isWhitespace( charAt ) ) {
                if( pi < 0 ) pi = i;
                t.append( Character.toLowerCase( charAt ) );
            } else {
                if( t.length() > 0 ) {
                    res.add( new Span( t.toString(), pi, i ) );
                    pi = -1;
                    t.setLength( 0 );
                }
            }
        }
        if( t.length() > 0 ) res.add( new Span( t.toString(), pi, length ) );
        return res.toArray( new Span[ 0 ] );
    }

    /**
     * Removes non digit or letter characters from an input string
     * This method accumulates chracters in a string buffer for efficiency
     *
     * @param args string to normalize
     * @return processed string
     */
    public static String normalizeFast( String args ) {
        final StringBuilder t = new StringBuilder();
        final int length = args.length();
        boolean inSpace = false;
        for( int i = 0; i < length; i++ ) {
            char charAt = args.charAt( i );
            if( Character.isLetterOrDigit( charAt ) ) {
                if( inSpace ) t.append( ' ' );
                t.append( Character.toLowerCase( charAt ) );
                inSpace = false;
            } else if( t.length() > 0 ) inSpace = true;
        }
        return t.toString();
    }

    /**
     * Normalizes a string using regular expressions. Might not be the most efficient way to do this, but it is flexible.
     * It overwrites the string argument variable
     *
     * @param norm string to normalize
     * @return processed string
     */
    public static String normalizeRegExp( String norm ) {
        norm = SPACE.matcher( norm ).replaceAll( SUBST_SPACE );
        norm = Normalizer.normalize( norm, Normalizer.Form.NFD );
        norm = DIACRITICS.matcher( norm ).replaceAll( SUBST_EMPTY );
        return norm.toLowerCase().trim();
    }

    /**
     * Normalizes a string using regular expressions. Might not be the most efficient way to do this, but it is flexible.
     * It doesn't overwrite the string argument variable
     *
     * @param str string to normalize
     * @return processed string
     */
    public static String normalizeRegExpCopy( String str ) {
        String norm = str;
        norm = PUNCT.matcher( norm ).replaceAll( SUBST_EMPTY );
        norm = SPACE.matcher( norm ).replaceAll( SUBST_SPACE );
        return norm.toLowerCase().trim();
    }

    /**
     * Extracts the modifiers from a query. Given a query and an entity linked in the query it maps the entity to
     * its "canonical form" (this is, its Wiki id), removing any possible type information from the Wiki id, this is
     * Tennis_(band) would be mapped to Tennis
     * Then it tries to remove the canonical name from the alias to which the entity is linked to in the query, and returns
     * any words left in the query after this process.
     *
     * @param q query to normalize
     * @param result entity id linked in the query
     * @return modifiers of the query (intent)
     */
    public static String getIntentPart( String q, String result ) {
        String normalizedId = result.replaceAll( "(.*?)(%28)" + "(.*?)" + "(%29.*)", "$1" );
        normalizedId = StringUtils.remove( normalizedId, "%3A" );
        normalizedId = Normalize.normalize( normalizedId.replaceAll( "[^A-Za-z0-9]", " " ) );
        for( String sw : ID_SW ) {
            normalizedId = normalizedId.replaceAll( sw, " " );
            q = q.replaceAll( sw, " " );
        }
        return StringUtils.remove( q, normalizedId );
    }

    public static void main( String args[] ) {
        String test = "ad. - asd. ; ; ; ;asdf assssXxvv.com hola.com .com one two     three four   ";
        System.out.println( test );
        System.out.println( Normalize.normalize( test ) );
        for( Span a : Normalize.normalizeWithSpans( test ) ) {
            System.out.println( a.getStartOffset() + "-" + a.getEndOffset() + " >" + a.span + "<:>" + test.subSequence( a.getStartOffset(), a.getEndOffset() ) + "<" );
        }
    }
}
