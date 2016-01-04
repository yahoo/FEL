package com.yahoo.semsearch.fastlinking.entityranker;

import java.util.Arrays;
import java.util.Comparator;

import com.yahoo.semsearch.fastlinking.hash.QuasiSuccinctEntityHash;
import com.yahoo.semsearch.fastlinking.view.CandidatesInfo;
import com.yahoo.semsearch.fastlinking.view.Entity;
import com.yahoo.semsearch.fastlinking.view.EntityContext;
import com.yahoo.semsearch.fastlinking.view.EntityScore;

/**
 * Makes use of the remaining words of the query to create a score for the
 * candidate This only computes zero-order dependencies (if there is a
 * dependency from a previous pick, it has to the factored out of this class).
 *
 * @author roi
 */
public class ContextualRanker extends ProbabilityRanker {
    protected final float normalizerQueries;
    protected final float normalizerAnchor;
    final double mu = 10;
    protected final float minContext = -30;
    protected final float corr = ( float ) 1;

    public ContextualRanker( QuasiSuccinctEntityHash hash ) {
        super( hash );
        normalizerQueries = hash.stats.SQEF + hash.stats.entities;
        normalizerAnchor = hash.stats.SLET + hash.stats.entities;
    }

    protected float queryNormalizer = 1;

    /**
     * Method with dynamic pruning. It operates in two phases, the first one
     * scores all the candidates as usual and the second phase stops whenever
     * the top-k candidate set can't change anymore int k number of surviving
     * candidates for the second phase (set to a negative number if you want all
     * of them scored)
     */
    @Override
    public EntityScore getHighestRankedEntity( CandidatesInfo infos, EntityContext context, String surfaceForm, int length, int k ) {
        if( infos.maxScore != null ) return infos.maxScore;
        double maxS = -Float.MAX_VALUE;
        final int l = infos.entities.length;
        final float[] scores = firstPhaseRanking( infos.entities, infos.QAF, infos.QAT, infos.QAC, infos.LAF, infos.LAT, l, surfaceForm );
        //first phase
        Integer[] idx = new Integer[ l ];
        for( int i = 0; i < l; i++ )
            idx[ i ] = i;

        Arrays.sort( idx, new Comparator<Integer>() {
            public int compare( Integer i1, Integer i2 ) {
                return -Double.compare( scores[ i1 ], scores[ i2 ] );
            }
        } );

        Entity maxE = null;

        //query normalizer
        queryNormalizer = context.queryNormalizer();

        //second phase
        for( int i = 0; i < l && i < k; i++ ) {
            final float eScore = scores[ idx[ i ] ];
            final Entity e = infos.entities[ idx[ i ] ];
            if( eScore < maxS ) { //max ctx = 1 nobody else can win
                break;
            }
            double s = secondPhaseRanking( e, length, eScore, context, surfaceForm, infos );
            if( s > maxS ) {
                maxS = s;
                maxE = e;
            }
        }
        infos.maxScore = new EntityScore( maxE, maxS );
        return infos.maxScore;
    }

    /**
     * Assigns scores for first phase ranking to an array of entities, using
     * global and local statistics. This method should be fast, as it scores
     * fully all the candidates for the segments.
     *
     * @param entities
     * @param QAF
     * @param QAT
     * @param QAC
     * @param LAF
     * @param LAT
     * @param l
     * @param surfaceForm
     * @return
     */
    public float[] firstPhaseRanking( Entity entities[], double QAF, double QAT, double QAC, double LAF, double LAT, int l, String surfaceForm ) {
        float[] scores = new float[ l ];
        final float priorQ = ( float ) ( ( QAT + 1 ) / ( QAT + LAT + 2 ) );
        final float priorA = 1 - priorQ;
        final float p_l_a_w = ( float ) ( ( LAF + 1 ) / ( LAT + 2 ) );

        float p_l_a = ( float ) ( ( QAC + 1 ) / ( QAT + 2 ) );
        for( int i = 0; i < l; i++ ) {
            Entity e = entities[ i ];
            float p_e = ( float ) ( ( e.QEF + 1 ) / ( normalizerQueries ) );
            float p_e_w = ( float ) ( ( e.LET + 1 ) / ( normalizerAnchor ) );
            float p_e_l_a_w = ( float ) ( ( e.LAET + mu * p_e_w ) / ( LAF + mu ) );
            float p_e_l_a = ( float ) ( ( e.QAEF + mu * p_e ) / ( QAF + mu ) );
            if( p_e_l_a_w > 1 ) p_e_l_a_w = 1; //TODO remove when the datapack is fixed
            if( p_e_l_a > 1 ) p_e_l_a = 1;
            scores[ i ] = priorQ * ( p_l_a * p_e_l_a + ( 1 - p_l_a ) * p_e ) + priorA * ( p_l_a_w * p_e_l_a_w + ( 1 - p_l_a_w ) * p_e_w );
        }
        return scores;
    }

    /**
     * Computes a second phase score for one entity. Subclasses must re-implement this method to redefine the scoring mechanism
     *
     * @param e
     * @param len
     * @param score
     * @param context
     * @param surfaceForm
     * @param info
     * @return
     */
    public double secondPhaseRanking( Entity e, int len, float score, EntityContext context, String surfaceForm, CandidatesInfo info ) {
        float contextScore = ( float ) context.getEntityContextScore( e ); //log-p, score is p
        //System.out.println( hash.getEntityName( e.id ) + " 1st [ " +  score + " ] " + " ctx [ " + contextScore + "] + Math.log(score)= " + Math.log( score ) + "" +
          //      "total "+ ( contextScore * corr + Math.log( score ) ) ) ;

        contextScore = contextScore < minContext ? minContext : contextScore;
        //float lenPrior = 1F / ( 1F + (float) Math.exp( -1 * ( len - 1 ) ) );
        double fScore = contextScore * corr + Math.log( score );
        return fScore;
    }

    /**
     * Old ranking version, without pruning
     */
    @Override
    public double rank( Entity e, CandidatesInfo info, EntityContext context, String surfaceForm, int length ) {

        final float priorQ = ( float ) ( ( info.QAT + 1 ) / ( info.QAT + info.LAT + 2 ) );
        final float priorA = 1 - priorQ;
        final float p_l_a_w = ( float ) ( ( info.LAF + 1 ) / ( info.LAT + 2 ) );

        float p_l_a = ( float ) ( ( info.QAC + 1 ) / ( info.QAT + 2 ) );
        float p_e = ( float ) ( ( e.QEF + 1 ) / ( normalizerQueries ) );
        float p_e_w = ( float ) ( ( e.LET + 1 ) / ( normalizerAnchor ) );
        float p_e_l_a_w = ( float ) ( ( e.LAET + mu * p_e_w ) / ( info.LAF + mu ) );
        float p_e_l_a = ( float ) ( ( e.QAEF + mu * p_e ) / ( info.QAF + mu ) );
        if( p_e_l_a_w > 1 ) p_e_l_a_w = 1; //TODO remove when the datapack is fixed
        if( p_e_l_a > 1 ) p_e_l_a = 1;
        float s1 = priorQ * ( p_l_a * p_e_l_a + ( 1 - p_l_a ) * p_e ) + priorA * ( p_l_a_w * p_e_l_a_w + ( 1 - p_l_a_w ) * p_e_w );
        queryNormalizer = context.queryNormalizer();
        double s = secondPhaseRanking( e, length, s1, context, surfaceForm, info );

        return s;
    }
}
