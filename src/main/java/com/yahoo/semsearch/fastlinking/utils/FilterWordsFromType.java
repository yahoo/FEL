/**
 Copyright 2016, Yahoo Inc.
 Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 **/

package com.yahoo.semsearch.fastlinking.utils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;

/**
 * This class processes a file of the format type <TAB> token_1 <CNTRL-A>
 * token_1_freq (<TAB> token_i <CNTRL-A> token_i_freq)*
 * 
 * The class implements a method to join types (integer id) with a file that contains integer to string id type
 * mapping and filters out those types that do not occur in an editorial file 
 * 
 * @author roi blanco
 *
 */
public class FilterWordsFromType {
      
    /**
     * Reads a file that contains words for different types and filter those that  
     * @param args command line arguments
     * @throws Exception
     */
    public static void main( String[] args ) throws Exception {
		SimpleJSAP jsap = new SimpleJSAP( FilterWordsFromType.class.getName(), "Filters a type-to-word file", new Parameter[] {
			new FlaggedOption( "input", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'i', "input", "type to words file" ),
			new FlaggedOption( "type", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 't', "type", "type id mapping file" ),
		} );
		JSAPResult jsapResult = jsap.parse( args );
		if( jsap.messagePrinted() ) return;

		BufferedReader lines = new BufferedReader( new FileReader( jsapResult.getString( "type" ) ) );
		String line = null;
		HashMap<Integer, String> typeMap = new HashMap<Integer, String>();
		while( ( line = lines.readLine() ) != null ) {
			if( line.isEmpty() ) continue;
			String parts[] = line.split( "\t" );
			typeMap.put( Integer.parseInt( parts[ 0 ] ), parts[ 2 ].trim() );
		}
		lines.close();

		lines = new BufferedReader( new FileReader( jsapResult.getString( "input" ) ) );

		line = null;
		while( ( line = lines.readLine() ) != null ) {
			if( line.isEmpty() ) continue;
			String parts[] = line.split( "\t" );
			String typeID = typeMap.get( Integer.parseInt( parts[ 0 ] ) ).trim();

			final int l = parts.length - 1;
			Integer[] idx = new Integer[ l ];
			String[] wordsArray = new String[ l ];
			int[] freqs = new int[ l ];
			for( int i = 1; i < parts.length; i++ ) {
				String[] words = parts[ i ].split( "\u0001" );
				int freq = Integer.parseInt( words[ 1 ] );
				idx[ i - 1 ] = i - 1;
				freqs[ i - 1 ] = freq;
				wordsArray[ i - 1 ] = words[ 0 ];
			}

			final int[] fFreq = freqs;
			Arrays.sort( idx, new Comparator<Integer>() {
				public int compare( Integer i1, Integer i2 ) {
					return -Integer.compare( fFreq[ i1 ], fFreq[ i2 ] );
				}
			} );

			int index = 0;
			System.out.print( typeID + "\t" );
			for( int i = 0; i < 5000; i++ ) {
				if( i == l ) break;
				if( wordsArray[ idx[ i ] ].contains( "_" ) ) {
					index = i;
					break;
				}
				System.out.print( wordsArray[ idx[ i ] ] + "\t" + fFreq[ idx[ i ] ] + "\t" );
			}

			for( int i = index; i < 5000; i++ ) {
				if( i == l ) break;
				System.out.print( wordsArray[ idx[ i ] ] + "\t" + fFreq[ idx[ i ] ] + "\t" );
			}
			System.out.println();
		}
		lines.close();
	}
}
