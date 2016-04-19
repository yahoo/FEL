/**
 Copyright 2016, Yahoo Inc.
 Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 **/

package com.yahoo.semsearch.fastlinking.io;

import java.io.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class WebscopeXmlReader implements DataReader {

    Set<Query> queries;
    LinkedHashMap<String, Session> sessions;
    HashMap<Session, Set<Query>> session2queries;
    HashMap<String, Query> id2query;
    int ID = 0;

    public WebscopeXmlReader() throws IOException {
        this( WebscopeXmlReader.class.getClass().getResourceAsStream( "/ydata-search-query-log-to-entities-v1_0-min10.xml" ) );
    }

    public WebscopeXmlReader( String filename ) throws IOException {
        this( new FileInputStream( filename ) );
    }

    public WebscopeXmlReader( InputStream is ) throws IOException {

        try {
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            Document doc = dBuilder.parse( is );
            readFile( doc );
        } catch( ParserConfigurationException e ) {
            throw new IOException( e );
        } catch( SAXException e ) {
            throw new IOException( e );
        }

    }

    private void readFile( Document doc ) throws IOException {

        queries = new TreeSet<Query>();
        sessions = new LinkedHashMap<String, Session>();
        session2queries = new HashMap<Session, Set<Query>>();
        id2query = new HashMap<String, Query>();

        doc.getDocumentElement().normalize();

        NodeList _sessions = doc.getDocumentElement().getChildNodes();

        for( int i = 0; i < _sessions.getLength(); i++ ) {

            Node session = _sessions.item( i );

            if( session.getNodeType() == Node.ELEMENT_NODE ) {

                String bcookie = null;
                Session currentSession = new Session();
                ArrayList<Query> currentEvents = new ArrayList<Query>();

                for( int j = 0; j < session.getAttributes().getLength(); j++ ) {

                    if( session.getAttributes().item( j ).getNodeName().equals( "id" ) ) {

                        bcookie = session.getAttributes().item( j ).getNodeValue();
                        currentSession = new Session( bcookie );

                    }
                }

                NodeList _queries = session.getChildNodes();

                for( int k = 0; k < _queries.getLength(); k++ ) {

                    Node query = _queries.item( k );

                    if( query.getNodeType() == Node.ELEMENT_NODE ) {

                        final Query q = new Query();
                        q.setBcookie( bcookie );
                        q.setID( Integer.toString( ++ID ) );

                        // quick and dirty fix to prevent duplicates
                        List<String> done = new ArrayList<String>();

                        for( int j = 0; j < query.getAttributes().getLength(); j++ ) {
                            //						     		 +> adult="false"
                            //						    	     +> ambiguous="false"
                            //						    	     +> assessor="19"
                            //						    	     +> cannot-judge="false"
                            //						    	     +> navigational="false"
                            //						    	     +> no-wp="false"
                            //						    	     +> non-english="false"
                            //						    	     +> quote-question="false"
                            //						    	     +> starttime="37.940637"

                            //System.out.println("     +> " + query.getAttributes().item(j));
                            if( query.getAttributes().item( j ).getNodeName().equals( "adult" ) && query.getAttributes().item( j ).getNodeValue().equals( "true" ) ) q.setAdult( true );

                            if( query.getAttributes().item( j ).getNodeName().equals( "ambiguous" ) && query.getAttributes().item( j ).getNodeValue().equals( "true" ) ) q.setAmbiguous( true );

                            if( query.getAttributes().item( j ).getNodeName().equals( "cannot-judge" ) && query.getAttributes().item( j ).getNodeValue().equals( "true" ) ) q.setCannot_judge( true );

                            if( query.getAttributes().item( j ).getNodeName().equals( "navigational" ) && query.getAttributes().item( j ).getNodeValue().equals( "true" ) ) q.setNavigational( true );

                            if( query.getAttributes().item( j ).getNodeName().equals( "no-wp" ) && query.getAttributes().item( j ).getNodeValue().equals( "true" ) ) q.setEntity_no_wp( true );

                            if( query.getAttributes().item( j ).getNodeName().equals( "non-english" ) && query.getAttributes().item( j ).getNodeValue().equals( "true" ) ) q.setNon_english( true );

                            if( query.getAttributes().item( j ).getNodeName().equals( "quote-question" ) && query.getAttributes().item( j ).getNodeValue().equals( "true" ) ) q.setQuote_or_question( true );

                            if( query.getAttributes().item( j ).getNodeName().equals( "assessor" ) ) q.setEditor( query.getAttributes().item( j ).getNodeValue() );

                            if( query.getAttributes().item( j ).getNodeName().equals( "starttime" ) ) q.setTimestamp( query.getAttributes().item( j ).getNodeValue() );

                        }

                        NodeList miscs = query.getChildNodes();

                        for( int l = 0; l < miscs.getLength(); l++ ) {

                            Node misc = miscs.item( l );

                            if( misc.getNodeType() == Node.ELEMENT_NODE ) {

                                if( misc.getNodeName().equals( "text" ) ) {
                                    q.setQuery( misc.getTextContent() );

                                } else if( misc.getNodeName().equals( "annotation" ) ) {

                                    final QuerySpan s = new QuerySpan();

                                    for( int j = 0; j < misc.getAttributes().getLength(); j++ ) {
                                        if( misc.getAttributes().item( j ).getNodeName().equals( "main" ) && misc.getAttributes().item( j ).getNodeValue().equals( "true" ) ) s.setMain( true );
                                    }

                                    NodeList targets = misc.getChildNodes();

                                    for( int a = 0; a < targets.getLength(); a++ ) {

                                        Node target = targets.item( a );

                                        if( target.getNodeType() == Node.ELEMENT_NODE ) {

                                            if( target.getNodeName().equals( "target" ) ) {
                                                s.setTag( target.getTextContent() );
                                            } else if( target.getNodeName().equals( "span" ) ) {
                                                s.setSpan( target.getTextContent() );
                                            }
                                        }
                                    }

                                    if( s.getTag().trim().length() > 0 && !done.contains( s.getTag() ) ) {
                                        q.spans.add( s );
                                        done.add( s.getTag() );
                                    }

                                } // end of annotation
                            }
                        } // end of miscs

                        queries.add( q );

                        currentEvents.add( q );
                        currentSession.setEvents( currentEvents.toArray( new Query[ 0 ] ) );
                        sessions.put( q.getBcookie(), currentSession );

                    } // end of query
                } // end of queries
            } // end of session
        } // end of sessions

        for( Query q : queries ) {

            Session s = sessions.get( q.getBcookie() );
            Set<Query> queries = session2queries.containsKey( s ) ? queries = session2queries.get( s ) : new TreeSet<Query>();
            queries.add( q );
            session2queries.put( s, queries );

            id2query.put( q.getID(), q );

        }
    }

    @Override
    public Set<Query> getQueries() {
        return queries;
    }

    @Override
    public Collection<Session> getSessions() {
        return sessions.values();
    }

    @Override
    public Set<Query> getQueriesForSession( Session s ) {
        return session2queries.get( s );
    }

    @Override
    public Query getQueryForId( String qid ) {
        return id2query.get( qid );
    }

    @Override
    public void saveQrels( String f ) {

        BufferedWriter writer = null;

        try {
            writer = new BufferedWriter( new FileWriter( f ) );

            for( Query q : getQueries() ) {

                for( QuerySpan s : q.getSpans() ) {

                    //					if (s.getTag().trim().length() > 1) {

                    //					if (!done.contains(line)) {
                    writer.write( q.getID() + " Q0 " + s.getTag().replaceAll( " ", "_" ) + " 1\n" );

                    //					}

                }
            }

            System.err.println( f + " written." );

        } catch( Exception e ) {
            e.printStackTrace();
        } finally {
            try {
                writer.close();
            } catch( Exception e ) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void saveQueries( String f ) {
        BufferedWriter writer = null;

        try {
            writer = new BufferedWriter( new FileWriter( f ) );

            for( Query q : getQueries() ) {
                writer.write( q.getID() + "\t" + q.getQuery() + "\n" );
            }

            System.err.println( f + " written." );

        } catch( Exception e ) {
            e.printStackTrace();
        } finally {
            try {
                writer.close();
            } catch( Exception e ) {
                e.printStackTrace();
            }
        }

    }

    /**
     * @param args
     * @throws Exception
     */
    public static void main( String[] args ) throws Exception {

        DataReader dr = new WebscopeXmlReader( args[ 0 ]);
        int total = 0;

        for( Session s : dr.getSessions() ) {

            System.out.println();
            System.out.println( s.getUser() );

            for( Query q : dr.getQueriesForSession( s ) ) {

                if( q.isNavigational() ) continue;

                total++;
                System.out.println( "  " + q.getQuery() );

                for( QuerySpan qs : q.getSpans() ) {
                    if( qs.isMain() ) System.out.println( "  * " + qs.getTag() + "\t" + qs.getSpan() );
                    else System.out.println( "  + " + qs.getTag() + "\t" + qs.getSpan() );
                }

                System.out.println();

            }

        }

        System.out.println( total + " queries" );

    }

    @Override
    public void saveSessions( String filename ) {
        // TODO Auto-generated method stub
    }
}
