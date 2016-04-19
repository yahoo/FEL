/**
 Copyright 2016, Yahoo Inc.
 Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 **/

package com.yahoo.semsearch.fastlinking.udf;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.text.Normalizer;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

/**
 * Class with text normalization functions for pig
 */
public class NormalizeText extends EvalFunc<String> {
    public static final String ENCODING = "UTF-8";

    public static final Pattern DIACRITICS = Pattern.compile( "[\\p{InCombiningDiacriticalMarks}\\p{IsLm}\\p{IsSk}]+" );

    public static final Pattern NON_ASCII = Pattern.compile( "[^\\p{ASCII}]+" );
    public static final Pattern SPACE = Pattern.compile( "[\\p{Space}\\p{Cntrl}]+" );
    public static final Pattern PUNCT = Pattern.compile( "\\p{Punct}+" );

    public static final String SUBST_SPACE = Matcher.quoteReplacement( " " );
    public static final String SUBST_EMPTY = Matcher.quoteReplacement( "" );

    public static String encode( String str ) throws UnsupportedEncodingException {
        return URLEncoder.encode( str, ENCODING );
    }

    public static String decode( String str ) throws UnsupportedEncodingException {
        return URLDecoder.decode( str, ENCODING );
    }

    public static String normalize( String str ) {
        String norm = str;
        // norm = NON_ASCII.matcher(norm).replaceAll(SUBST_SPACE);
        norm = PUNCT.matcher( norm ).replaceAll( SUBST_SPACE );
        norm = SPACE.matcher( norm ).replaceAll( SUBST_SPACE );

        return norm.toLowerCase().trim();
    }

    public static String normalizeToASCII( String str ) {
        str = Normalizer.normalize( str, Normalizer.Form.NFD );
        str = DIACRITICS.matcher( str ).replaceAll( SUBST_EMPTY );

        return str;

		/*
		 * String norm = Normalizer.normalize(str, Normalizer.Form.NFKD);
		 * StringBuilder builder = new StringBuilder(); for(int i=0, j=0; i <
		 * str.length() && j < norm.length(); i++, j++) { char src = str.charAt(
		 * i ); char trg = norm.charAt( j ); builder.append(trg);
		 * 
		 * if(src != trg){ j++; } } return builder.toString().trim();
		 */
    }

    @Override
    public String exec( Tuple input ) throws IOException {
        if( input == null ) {
            throw new IOException( "No arguments: Usage NormalizeText(String, ASCII)" );
        }
        if( input.size() != 2 ) {
            throw new IOException( "Wrong arguments: Usage NormalizeText(String, ASCII)" );
        }
        for( int i = 0; i < input.size(); i++ ) {
            if( input.get( i ) == null ) {
                return null;
            }
        }

        if( Boolean.parseBoolean( ( String ) input.get( 1 ) ) ) {
            String norm = normalizeToASCII( ( String ) input.get( 0 ) );
            return normalize( norm );

        } else {
            return normalize( ( String ) input.get( 0 ) );
        }
    }

    public static void main( String[] args ) throws Exception {
        System.out.println( args[ 0 ] );
        System.out.println( normalize( args[ 0 ] ) );
        System.out.println( normalizeToASCII( args[ 0 ] ) );
    }
}
