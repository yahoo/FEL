/**
 Copyright 2016, Yahoo Inc.
 Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 **/

package com.yahoo.semsearch.fastlinking.utils;

import info.bliki.wiki.dump.IArticleFilter;
import info.bliki.wiki.dump.Siteinfo;
import info.bliki.wiki.dump.WikiArticle;
import info.bliki.wiki.dump.WikiXMLParser;
import info.bliki.wiki.filter.ITextConverter;
import info.bliki.wiki.filter.PlainTextConverter;
import info.bliki.wiki.model.WikiModel;
import it.unimi.dsi.io.FastBufferedReader;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.util.TextPattern;
import org.xml.sax.SAXException;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.net.URLEncoder;

/**
 * Extracts the first paragraph of every Wikipedia page
 * * @author roi blanco
 */
public class ExtractFirstParagraphs implements IArticleFilter {
    protected final String baseURL = "http://en.wikipedia.org/wiki/";
    protected final String linkBaseURL = String.valueOf( this.baseURL ) + "${title}";
    protected final String imageBaseURL = String.valueOf( this.baseURL ) + "${image}";
    protected final FastBufferedReader wordReader = new FastBufferedReader();
    protected final MutableString word = new MutableString();
    protected final MutableString nonWord = new MutableString();
    protected static final TextPattern BRACKETS_CLOSED = new TextPattern( ( CharSequence ) "]]" );
    protected static final TextPattern BRACES_CLOSED = new TextPattern( ( CharSequence ) "}}" );
    protected final FileWriter writer;
    private MutableString title = new MutableString();
    protected MutableString firstPar = new MutableString();

    public ExtractFirstParagraphs( String file ) throws IOException {
        this.writer = new FileWriter( file );
    }

    public static void main( String[] arg ) {
        if( arg.length < 2 ) {
            System.err.println( " USAGE java ExtractFirstParagraphs  <inputFile> <outputFile>" );
        }
        try {
            ExtractFirstParagraphs handler = new ExtractFirstParagraphs( arg[ 1 ] );
            WikiXMLParser wxp = new WikiXMLParser( arg[ 0 ], ( IArticleFilter ) handler );
            wxp.parse();
        } catch( Exception e ) {
            e.printStackTrace();
        }
    }

    @SuppressWarnings( "deprecation" )
    @Override
    public void process( WikiArticle article, Siteinfo siteinfo ) throws SAXException {
        this.title.length( 0 );
        this.firstPar.length( 0 );
        this.title.append( URLEncoder.encode( article.getTitle().replace( ' ', '_' ) ) );
        WikiModel wikiModel = new WikiModel( this.imageBaseURL, this.linkBaseURL );
        String plainText = wikiModel.render( new PlainTextConverter(), article.getText() );
        for( int start = 0; start < plainText.length(); ++start ) {
            if( Character.isWhitespace( plainText.charAt( start ) ) ) continue;
            if( plainText.charAt( start ) == '{' ) {
                if( ( start = BRACES_CLOSED.search( plainText, start ) ) == -1 ) break;
                ++start;
                continue;
            }
            if( plainText.charAt( start ) == '[' ) {
                if( ( start = BRACKETS_CLOSED.search( plainText, start ) ) == -1 ) break;
                ++start;
                continue;
            }
            int end = plainText.indexOf( 10, start );
            if( end <= start ) break;
            this.firstPar.append( plainText.substring( start, end ) );
            break;
        }
        this.word.length( 0 );
        this.nonWord.length( 0 );
        this.wordReader.setReader( ( Reader ) new FastBufferedReader( this.firstPar ) );
        try {
            this.writer.append( this.title ).append( "\t" );
            while( this.wordReader.next( this.word, this.nonWord ) ) {
                this.writer.append( this.word.toLowerCase() ).append( " " );
            }
            this.writer.append( "\n" );
        } catch( IOException e ) {
            e.printStackTrace();
        }
    }
}
