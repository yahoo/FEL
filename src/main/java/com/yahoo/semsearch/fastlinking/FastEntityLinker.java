package com.yahoo.semsearch.fastlinking;

import it.unimi.dsi.fastutil.io.BinIO;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.yahoo.semsearch.fastlinking.entityranker.EntityRelevanceJudgment;
import com.yahoo.semsearch.fastlinking.entityranker.NPMIRanker;
import com.yahoo.semsearch.fastlinking.entityranker.ProbabilityRanker;
import com.yahoo.semsearch.fastlinking.hash.AbstractEntityHash;
import com.yahoo.semsearch.fastlinking.hash.CountAndRecordStats;
import com.yahoo.semsearch.fastlinking.hash.QuasiSuccinctEntityHash;
import com.yahoo.semsearch.fastlinking.utils.Normalize;
import com.yahoo.semsearch.fastlinking.view.CandidatesInfo;
import com.yahoo.semsearch.fastlinking.view.EmptyContext;
import com.yahoo.semsearch.fastlinking.view.Entity;
import com.yahoo.semsearch.fastlinking.view.EntityContext;
import com.yahoo.semsearch.fastlinking.view.EntityScore;
import com.yahoo.semsearch.fastlinking.view.EntitySpan;
import com.yahoo.semsearch.fastlinking.view.Span;

/**
 * Entity linker class. this class uses an AbstractEntityHash to select candidates and proxies the scoring of
 * these candidates to a CandidateScorer
 * It contains two methods for selecting candidates, one based on dynamic programming and a greedy one. For details on these algorithms
 *
 * @author roi
 * @see <a href="http://labs.yahoo.com/publication/fast-and-space-efficient-entity-linking-in-queries/">fast entity linking on queries</a>
 */
public class FastEntityLinker {
    protected EntityContext context;
    private AbstractEntityHash hash;
    private CountAndRecordStats stats;
    public ProbabilityRanker ranker;
    private final boolean DEBUG = true;

    private static final String[] BAD_WORDS = new String[]{ "wiki", "com", "www" };
    private static final Set<String> FILTER = new HashSet<String>( Arrays.asList( BAD_WORDS ) );
    private double nilValueOne = -100;
    private final Entity nilEntity = new Entity( -1 );
    private EntityScore nilCandidate = new EntityScore( nilEntity, nilValueOne );
    private final Entity[] emptyEntities = new Entity[ 0 ];


    public FastEntityLinker( AbstractEntityHash hash, CountAndRecordStats stats, EntityContext context ) {
        this.stats = stats;
        this.hash = hash;
        this.ranker = new ProbabilityRanker( ( QuasiSuccinctEntityHash ) hash );
        this.context = context;
    }

    public FastEntityLinker( AbstractEntityHash hash, EntityContext context ) {
        this( hash, ( ( QuasiSuccinctEntityHash ) hash ).stats, context );
    }

    public void setNPMIRanker() {
        ranker = new NPMIRanker( ( QuasiSuccinctEntityHash ) hash );
    }

    /**
     * Removes the Wikipedia weight of the scoring function
     */
    public void removeWiki() {
        ranker.pQuery = 1;
        ranker.pAnchor = 0;
    }

    /**
     * Removes the query log weight of the scoring function
     */
    public void removeQuery() {
        ranker.pQuery = 0;
        ranker.pAnchor = 1;
    }

    /**
     * Inner class to store partial ranking results, a span, the alias and the score
     *
     * @author roi
     */
    public class EntityResult implements Comparable<EntityResult> {
        public Span s;
        public CharSequence text;
        public int id;
        public double score;

        private EntityResult( Span s, CharSequence text, int id, double score ) {
            this.id = id;
            this.text = text;
            this.s = s;
            this.score = score;
        }

        @Override
        public int compareTo( EntityResult o ) {
            return -Double.compare( score, o.score );
        }
    }

    /**
     * Given a query and a threshold, segments the query, detects the candidates and scores them. Returns a candidate only if it is above the threshold
     *
     * @param query
     * @param threshold
     * @return
     */
    public List<EntityResult> getResults( final String query, final double threshold ) {
        List<EntityResult> res = new ArrayList<EntityResult>();
        ArrayList<EntitySpan> entityAnnotation = getBestChunking( query, ranker, context );
        //ArrayList<EntitySpan> entityAnnotation = getBestChunkingMaxIterativeV2( query, ranker, context );
        for( EntitySpan span : entityAnnotation ) {
            if( span.e.id != -1 ) {
                Entity e = span.e;
                //if( DEBUG )
                  //  System.out.println( "query " + query + " span: \033[1m [" + span.getSpan() + "] \033[0m eId: " + e.id + " \033[1m " + hash.getEntityName( e.id ) + " \033[0m score= " + span.score );

                CharSequence text = hash.getEntityName( e.id );
                //if( span.span.length() > 2 && span.score > threshold )
                if(  span.score > threshold )
                    res.add( new EntityResult( span, text, e.id, span.score ) );
            }else{
            }
        }
        Collections.sort( res );
        return res;
    }

    /**
     * Max value of two double numbers
     *
     * @param a a number
     * @param b another number
     * @return
     */
    public double max( double a, double b ) {
        if( a > b ) return a;
        return b;
    }

    /**
     * Method used to set the proper per query context. Emtpy in FastEntityLinker but extended in subclasses
     *
     * @param parts string array holding the context
     * @param left  index where the context begins at
     * @param right index where the context ends at
     */
    public void setContext( Span[] parts, int left, int right ) { }

    /**
     * Method used to set the proper per query context. Emtpy in FastEntityLinker but extended in subclasses
     *
     * @param parts string array holding the context
     * @param left  index where the context begins at
     * @param right index where the context ends at
     */
    public void setContext( String[] parts, int left, int right ) { }

    /**
     * Selects a substring from a string array and returns it as a string
     *
     * @param parts
     * @param min
     * @param max
     * @return
     */
    public String chunk( String[] parts, int min, int max ) {
        StringBuilder res = new StringBuilder();
        res.append( parts[ min ] );
        for( int i = min + 1; i < max + 1; i++ ) {
            res.append( " " );
            res.append( parts[ i ] );
        }
        return res.toString();
    }

    /**
     * Selects a substring from a string array and returns it as a string
     *
     * @param parts
     * @param min
     * @param max
     * @return
     */
    public Span chunk( Span[] parts, int min, int max ) {
        StringBuilder res = new StringBuilder();
        res.append( parts[ min ] );
        for( int i = min + 1; i < max + 1; i++ ) {
            res.append( " " );
            res.append( parts[ i ] );
        }
        return new Span( res.toString(), parts[ min ].getStartOffset(), parts[ max ].getEndOffset() );
    }

    /**
     * Returns the score of a segment of a string array in between to indexes
     *
     * @param parts string array containing the query
     * @param min   lower index of the segment
     * @param max   higher index of the segment
     * @return
     */
    public EntityScore scoreSegment( Span[] parts, int min, int max ) {
        Span surfaceForm = chunk( parts, min, max );
        CandidatesInfo candidatesInfo = hash.getCandidatesInfo( surfaceForm.span );
        if( candidatesInfo == null ) return nilCandidate;
        return ranker.getHighestRankedEntity( candidatesInfo, context, surfaceForm.span, max - min, 10000 );
    }


    /**
     * Returns the score of a segment of a string array in between to indexes
     *
     * @param parts string array containing the query
     * @param min   lower index of the segment
     * @param max   higher index of the segment
     * @return
     */
    public EntityScore scoreSegment( String[] parts, int min, int max ) {
        String surfaceForm = chunk( parts, min, max );
        CandidatesInfo candidatesInfo = hash.getCandidatesInfo( surfaceForm );
        if( candidatesInfo == null ) return nilCandidate;
        return ranker.getHighestRankedEntity( candidatesInfo, context, surfaceForm, max - min, 10000 );
    }

    /**
     * Selects the chunking that maximizes the likelihood of the candidates
     * Given a sequence A1 A2 A3 ... AK the max score of any chunking is m(A1 A2
     * ... AK ) = m( m([A1]) + m(A2..AK), m([A1A2]) + m(A3 ... AK),
     * m([A1..AK-1])+m([AK]), m([A1...AK]) ) where [A1A2] is string resulting of
     * concatenating A1 and A2 and m([A1]) = s(A1) and m([A1A2]) = m([A1A2],
     * m(A1) + m(A2)
     *
     * @param q      query
     * @param ranker ranker to score candidates
     */

    private ArrayList<EntitySpan> getBestChunking( String q, ProbabilityRanker ranker, EntityContext context ) {
        Span parts[] = Normalize.normalizeWithSpans( q );
        final int l = parts.length;
        setContext( parts, 0, 0 );
        Entity[] ids = new Entity[ l + 1 ];
        double[] maxscores = new double[ l + 1 ];
        int[] previous = new int[ l + 1 ];
        double[] currentScores = new double[ l + 1 ];

        for( int i = 0; i < l + 1; i++ ) {
            currentScores[ i ] = nilValueOne;
            previous[ i ] = i;//new
        }

        for( int i = 0; i < l; i++ ) {
            for( int j = 0; j < i + 1; j++ ) {
                EntityScore candidate = scoreSegment( parts, j, i );
                double score = candidate.score + maxscores[ j ];
                if( score > currentScores[ i ] ) {
                    maxscores[ i ] = score;
                    currentScores[ i ] = candidate.score; //was score
                    previous[ i ] = j;
                    ids[ i ] = candidate.entity;
                }
            }
        }
        int i = l;
        int j;
        final ArrayList<EntitySpan> spanList = new ArrayList<EntitySpan>();

        while( i > 0 ) {
            i--;
            j = previous[ i ];
            Span segment = chunk( parts, j, i );//span to add
            if( ids[ i ] == null ) ids[ i ] = nilEntity;
            EntitySpan s = new EntitySpan( segment, ids[ i ], currentScores[ i ] );
            spanList.add( s );
            i = j;
        }
        return spanList;
    }

    /**
     * Links entities in a string iteratively while(there are more aliases to
     * link in Q) generate the best chunking of Q select the maximum scored
     * entity e in Q remove the alias a that generates e from Q
     * <p/>
     * Note that when removing a from Q the new Q would contain NIL tokens
     * instead of a ([a b c] \ b -> [a NIL c] and not [a c] ) To speed things up
     * this method computes all the possible scores and then iterates over them
     *
     * @param q       query
     * @param ranker  class to rank candidates
     * @param context variable to score the entity-query contexts
     * @return
     */
    public ArrayList<EntitySpan> getBestChunkingMaxIterativeV2( String q, ProbabilityRanker ranker, EntityContext context ) {
        CandidatesInfo nilCandidate = new CandidatesInfo( emptyEntities, 0, 0, 0, 0, 0 );
        nilCandidate.maxScore = new EntityScore( nilEntity, nilValueOne );
        nilEntity.id = -1; //if this works move these two out of this loop
        String parts[] = Normalize.normalize( q ).split( "\\s+" );

        ArrayList<String> finalWords = new ArrayList<String>();
        for( int i = 0; i < parts.length; i++ ) {
            if( !FILTER.contains( parts[ i ] ) ) finalWords.add( parts[ i ] );
        }

        parts = finalWords.toArray( new String[ finalWords.size() ] );
        final int l = parts.length;
        setContext( parts, 0, 0 );
        double[][] scores = new double[ l ][ l ];
        CandidatesInfo infos[][] = new CandidatesInfo[ l ][ l ];
        for( int j = 0; j < l; j++ ) {
            StringBuffer query = new StringBuffer( parts[ j ] );
            //setContext( parts, j , j + 1 ); //comment this one if you want full-context
            if( query.toString().length() > 1 ) {
                CandidatesInfo candidatesInfo = hash.getCandidatesInfo( query.toString() );
                if( candidatesInfo != null ) {
                    scores[ 0 ][ j ] = ranker.getHighestRankedEntity( candidatesInfo, context, query.toString(), 1, 10000 ).score;
                    infos[ 0 ][ j ] = candidatesInfo;
                } else {
                    scores[ 0 ][ j ] = nilValueOne;
                    infos[ 0 ][ j ] = nilCandidate;
                }
            } else {
                scores[ 0 ][ j ] = nilValueOne;
                infos[ 0 ][ j ] = nilCandidate;
            }
            for( int i = 1; j + i < l; i++ ) {
                query.append( " " );
                query.append( parts[ i + j ] );
                CandidatesInfo candidatesInfo = hash.getCandidatesInfo( query.toString() );

                //
                if( candidatesInfo != null ) {
                    scores[ i ][ j ] = ranker.getHighestRankedEntity( candidatesInfo, context, query.toString(), i + 1, 10000 ).score;
                    infos[ i ][ j ] = candidatesInfo;
                } else {
                    scores[ i ][ j ] = nilValueOne;//* ( i + 1 );
                    infos[ i ][ j ] = nilCandidate;
                }
            }
        }

        double maxScores[][] = new double[ l ][ l ];

        for( int i = 0; i < l; i++ )
            for( int j = 0; j < l; j++ )
                maxScores[ i ][ j ] = nilValueOne;//* ( i + 1 );

        int splits[][] = new int[ l ][ l ];
        for( int i = 0; i < l; i++ ) { // m(a) = s(a)
            maxScores[ 0 ][ i ] = scores[ 0 ][ i ];
            splits[ 0 ][ i ] = 1;
        }
        for( int i = 0; i < l + 1; i++ ) { // for every sub-sequence length (i+1)
            for( int j = 0; i + j < l; j++ ) { // for every starting point j
                double maxS = scores[ i ][ j ]; //score of the whole sub-sequence
                int bestSplit = i + 1; //where do we split
                for( int k = 0; k < i; k++ ) { // split in i chunks
                    double score = max( scores[ k ][ j ], maxScores[ i - k - 1 ][ j + k + 1 ] );
                    if( score > maxS ) {
                        maxS = score;
                        bestSplit = k + 1;
                    }
                }
                maxScores[ i ][ j ] = maxS;
                splits[ i ][ j ] = bestSplit;
            }
        }

        boolean done = false;
        int i = l - 1;
        int j = 0; //we are at position j and have l - 1 items to consume
        final ArrayList<EntitySpan> spanList = new ArrayList<EntitySpan>();

        while( !done ) { //follow the trail
            final int jump = splits[ i ][ j ];
            final StringBuffer sb = new StringBuffer();
            for( int z = 0; z < jump; z++ ) {
                sb.append( parts[ z + j ] );
                sb.append( " " );
            }
            EntitySpan s = new EntitySpan( sb.toString() );
            spanList.add( s );
            s.score = infos[ jump - 1 ][ j ].maxScore.score;
            s.e = infos[ jump - 1 ][ j ].maxScore.entity;
            j += jump;
            if( j > l - 1 ) done = true;

            i -= jump;
        }
        return spanList;
    }

    /**
     * Getter
     *
     * @return the current context
     */
    public EntityContext getContext() {
        return context;
    }


    /**
     * generates a number of candidates per spot for a given query and scores them using the previously set ranker
     *
     * @param q                 query
     * @param candidatesPerSpot number of candidates to score per spot
     * @param context           class to score the context
     * @return
     * @throws InterruptedException
     */
    public ArrayList<EntityScore> generateAllCandidates( String q, int candidatesPerSpot, EntityContext context ) throws InterruptedException {
        ArrayList<EntityScore> allCandidates = new ArrayList<EntityScore>();
        String parts[] = Normalize.normalize( q ).split( "\\s+" );
        final int l = parts.length;
        ArrayList<String> ctxWords = new ArrayList<String>();
        for( String p : parts )
            ctxWords.add( p );
        context.setContextWords( ctxWords );
        for( int i = 0; i < l; i++ ) {
            StringBuilder text = new StringBuilder();
            for( int j = i; j < l; j++ ) {
                text.append( parts[ j ] );
                CandidatesInfo infos = hash.getCandidatesInfo( text.toString() );
                if( infos != null ) {
                    ArrayList<EntityScore> score = ranker.getTopKEntities( infos, context, q, ( j - i ), candidatesPerSpot );
                    allCandidates.addAll( score );
                }
                text.append( " " );
            }
        }
        Collections.sort( allCandidates );
        return allCandidates;
    }

    /**
     * Generates all the possible candidates for a given query and scores them
     * This method is O(|q|^2)
     *
     * @param query
     * @param k     number of top candidates to return. This number is global for the whole query. If you want k candidates per span use generateAllCandidates
     * @return
     * @throws InterruptedException
     */
    public List<EntityResult> getResultsGreedy( final String query, int k ) throws InterruptedException {
        List<EntityResult> res = new ArrayList<EntityResult>();
        ArrayList<EntityScore> scores = generateAllCandidates( query, k, context );
        int i = 0;
        while( i < k && i < scores.size() ) {
            EntityScore s = scores.get( i );
            CharSequence n = hash.getEntityName( s.entity.id );
            if( !Double.isNaN( s.score ) ) { //&& n.length() > 2 ) { //TODO remove length restriction
                res.add( new EntityResult( new EntitySpan( query ), n, s.entity.id, s.score ) );
            } else {
                k++;
            }
            i++;
        }
        return res;
    }

    /**
     * Main method that provides a command-line input linker
     *
     * @param args
     * @throws Exception
     */
    public static void main( String args[] ) throws Exception {
        double threshold = -30;
        QuasiSuccinctEntityHash hash = ( QuasiSuccinctEntityHash ) BinIO.loadObject( args[ 0 ] );
        final BufferedReader br = new BufferedReader( new InputStreamReader( System.in ) );
        String q;
        FastEntityLinker fel = new FastEntityLinker( hash, hash.stats, new EmptyContext() );
        for(; ; ) {
            System.out.print( ">" );
            q = br.readLine();
            if( q == null ) {
                System.err.println();
                break; // CTRL-D
            }
            if( q.length() == 0 ) continue;
            String[] parts = q.split( "\t" );
            q = parts[ 0 ];
            String loc = "";
            if( parts.length > 1 ) {
                q = parts[ 0 ];
                loc = parts[ 1 ];
            }

            long time = -System.nanoTime();
            try {
                List<EntityResult> results = fel.getResults( q, threshold );
                //List<EntityResult> results = fel.getResultsGreedy( q, 10 );
                //List<EntityScore> results = fel.generateAllCandidates( q, 10, fel.context );
                //List<EntityResult> results = fel.getResultsGreedy( q, 5 );
                //for( EntityScore es : results ) {
                 //   System.out.println( q + "\t" + fel.hash.getEntityName( es.entity.id ) + "\t" + es.score + "\t" + es.entity.id );
               // }
           for ( EntityResult er : results ) {
   		    //    System.out.println( "Wiki Id: \033[1m [" + er.text + "] \033[0m eId:" + er.id + " score: " + er.score + " (" + er.s.span + ")" ) ;
   		    System.out.println( q + "\t" + loc + "\t" + er.text + "\t" + er.score + "\t" + er.id );
   		}
                time += System.nanoTime();
                System.out.println( "Time to rank and print the candidates:" + time / 1000000. + " ms" );
            } catch( Exception e ) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Given a list of entity relevance judgments returns the EntityRelevanceJudgment for a given entity (checks if the entity is in the list
     * otherwise it returns UNKNOWN).
     *
     * @param entity
     * @param j
     * @return
     * @throws UnsupportedEncodingException
     */
    public static EntityRelevanceJudgment relevanceOfEntity( CharSequence entity, ArrayList<EntityRelevanceJudgment> j ) throws UnsupportedEncodingException {
        //entity = URLDecoder.decode( entity.toString(), "UTF-8" );
        for( EntityRelevanceJudgment ju : j ) {
            if( entity.equals( ju.id ) ) return ju;
        }
        return notFound;
    }

    private static EntityRelevanceJudgment notFound = new EntityRelevanceJudgment( "UNK", "NF", false, "0" );

    /**
     * Reads a document with the following format query url grade
     * "auto insurance""""":2418787 auto insurance:local:45946102 Perfect 1204
     * alvarado terrace:0 alvarado:yk:Alvarado,_Texas Bad
     * <p/>
     * Takes as the query the first part (ignores the location, after the :) As
     * the id, removes everything local and takes the part after the ":" It
     * assumes that the file is already query-sorted
     *
     * @param file Input file with the training data
     * @throws IOException
     */
    public static HashMap<String, ArrayList<EntityRelevanceJudgment>> readTrainingDataALP( String file ) throws IOException {
        HashMap<String, ArrayList<EntityRelevanceJudgment>> data = new HashMap<String, ArrayList<EntityRelevanceJudgment>>();
        final BufferedReader br = new BufferedReader( new InputStreamReader( new FileInputStream( file ) ) );
        String line;
        String previousQuery = "";
        ArrayList<EntityRelevanceJudgment> juds = null; //java compiler-happy
        line = br.readLine();//header
        int i = 0;
        try {
            while( ( line = br.readLine() ) != null ) {
                i++;
                String parts[] = line.split( "\\t" );
                if( !isANumber( parts[ 1 ] ) ) {
                    if( !parts[ 0 ].equals( previousQuery ) ) {
                        previousQuery = parts[ 0 ];
                        juds = new ArrayList<EntityRelevanceJudgment>();
                        data.put( parts[ 0 ], juds );
                    }
                    EntityRelevanceJudgment e = new EntityRelevanceJudgment( parts[ 1 ], parts[ 2 ], isRel( parts[ 2 ] ), parts[ 3 ] );
                    juds.add( e );
                }
            }
        } catch( ArrayIndexOutOfBoundsException e ) {
            String[] p = line.split( "\\t" );
            System.out.println( "wrong line #" + i + ": " + line + " len " + p.length + " line[0] " + p[ 0 ] + " line[1] " + p[ 1 ] );
            System.exit( -1 );
        }
        br.close();

        return data;
    }

    /**
     * Returns true if the string looks like a number
     *
     * @param s
     * @return
     */
    private static boolean isANumber( String s ) {
        return s.matches( "[-+]?\\d*\\.?\\d+" );
    }

    /**
     * Defines binary relevance based on a label string
     */
    private static boolean isRel( String label ) {
        if( label.toLowerCase().equals( "bad" ) ) return false;
        if( label.toLowerCase().equals( "fair" ) ) return false; //we only want > Fair
        return true;
    }

}
