package com.yahoo.semsearch.fastlinking;

import com.yahoo.semsearch.fastlinking.hash.QuasiSuccinctEntityHash;
import com.yahoo.semsearch.fastlinking.utils.EntityLinkingUtils;
import com.yahoo.semsearch.fastlinking.view.EntityContext;
import com.yahoo.semsearch.fastlinking.w2v.CompressedSingleFileEntitySimilarityUtil;
import com.yahoo.semsearch.fastlinking.w2v.LREntityContext;

import it.unimi.dsi.fastutil.io.BinIO;

import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealVector;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.ListIterator;

/**
 * Created by aasishkp on 12/15/15.
 *
 * This class takes entity-mentions and n-best list of entity-links for each entity mention as input. Constructs a lattice from the nbest lists
 * and runs Forward-Backward algorithm.
 * J. Binder, K. Murphy and S. Russell. Space-Efficient Inference in Dynamic Probabilistic Networks. Int'l, Joint Conf. on Artificial Intelligence, 1997.

 java -Xmx512m -Xmx10g exec:java -Dexec.mainClass=com.yahoo.semsearch.fastlinking.CoherentEntityLinker -Dexec.args="enwiki.wiki2vec.d300.compressed WIKIHASH.nov ENTITIES.PHRASE.model PHRASE.model"  -Dexec.classpathScope=compile
 */



/**
 * Struct class with logSimilarity and path to return
 */
class DPSearch {

    public double[][] logSimilarity;
    public String[][] path;

    public DPSearch(double[][] logSimilarity, String[][] path) {
        this.logSimilarity = logSimilarity;
        this.path = path;
    }
}


public class CoherentEntityLinker {

    private static final int NBEST = 10;
    private CompressedSingleFileEntitySimilarityUtil gensimEntityEmbeddings = null;

    public int getMAXNBEST() {
        return MAXNBEST;
    }

    public void setMAXNBEST(int MAXNBEST) {
        this.MAXNBEST = MAXNBEST;
    }

    private int MAXNBEST = 10;

    public double getLEXSIM_LAMBDA() {
        return LEXSIM_LAMBDA;
    }

    public void setLEXSIM_LAMBDA(double LEXSIM_LAMBDA) {
        this.LEXSIM_LAMBDA = LEXSIM_LAMBDA;
    }

    private double LEXSIM_LAMBDA = 0.5;

    private  EntityContextFastEntityLinker wikiLinker;


    /**
     *
     * @param entityEmbeddings
     */
    public CoherentEntityLinker(String entityEmbeddings, String hashFile, String unigramFile, String entityFile) {
        try {
            gensimEntityEmbeddings = new CompressedSingleFileEntitySimilarityUtil(entityEmbeddings);
            EntityContext queryContext;
            QuasiSuccinctEntityHash hash = (QuasiSuccinctEntityHash) BinIO.loadObject(hashFile);
            queryContext = new LREntityContext(unigramFile, entityFile, hash);
            wikiLinker = new EntityContextFastEntityLinker(hash, hash.stats, queryContext);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    /**
     * Link a query to its corresponding NBEST wiki entities, via {@link EntityContextFastEntityLinker#getResultsGreedy(String, int)}.
     * e.g. getWikiLinks("Apple") returns ["Apple_Inc", "Apple_(fruit)"]
     *
     * @param query is an entity span of text
     * @return an ordered list of wiki links (best to worst) for that query
     */
    private ArrayList<String> getWikiLinks(String query) {
        ArrayList<String> wikiLinks = new ArrayList<String>();
        List<FastEntityLinker.EntityResult> results = null;
        if (wikiLinker != null) {
            try {
                results = wikiLinker.getResultsGreedy(query, NBEST);
            } catch (InterruptedException e) {
            }
            if (results != null) {
                for (FastEntityLinker.EntityResult er : results) {
                    String wikiEntity = er.text.toString();
                    wikiEntity = EntityLinkingUtils.hexadecimalToChar(wikiEntity);
                    wikiLinks.add(wikiEntity);
                }
            }
        }
        return wikiLinks;
    }


    public static void main(String[] args) {
        String[] A0 = new String[]{"Huma_Abedin", "Huma_bird", "Uniform_memory_access"};
        String[] A1 =
            new String[]{"Hillary_Clinton", "Bill_Clinton", "Edmund_Hillary", "Political_positions_of_Hillary_Clinton",
                         "Clinton_Iowa", "Clinton_Massachusetts"
            };
        String[]
            A2 =
            new String[]{"Bill_Clinton", "Hillary_Clinton", "Clinton_Iowa", "Clinton_Massachusetts",
                         "Clinton_Mississippi"
            };
        String[] A3 = new String[]{"Elijah_Cummings", "Elijah", "Jason_Cummings", "Bobby_Cummings"};
        String[] A4 = new String[]{"Huma_Abedin", "Minhajul_Abedin"};

        String[]
            A5 =
            new String[]{"Bill_Clinton", "Hillary_Clinton", "Clinton_Iowa", "Clinton_Massachusetts",
                         "Clinton_Mississippi"};

        String[]
            surfaceStrings =
            new String[]{"Abedin", "Hillary Clinton", "Clinton", "Elijah Cummings", "Abedin", "Clinton"};

        CoherentEntityLinker coherentEntityLinker = new CoherentEntityLinker(args[0], args[1], args[2], args[3]);

        int lenSurfaceStrings = surfaceStrings.length;
        ArrayList<String> nBestList[] = new ArrayList[]{new ArrayList<String>(Arrays.asList(A0)),
                                                        new ArrayList<String>(Arrays.asList(A1)),
                                                        new ArrayList<String>(Arrays.asList(A2)),
                                                        new ArrayList<String>(Arrays.asList(A3)),
                                                        new ArrayList<String>(Arrays.asList(A4)),
                                                        new ArrayList<String>(Arrays.asList(A5))};

        ArrayList<String> nBestListReverse[] = new ArrayList[]{new ArrayList<String>(Arrays.asList(A5)),
                                                               new ArrayList<String>(Arrays.asList(A4)),
                                                               new ArrayList<String>(Arrays.asList(A3)),
                                                               new ArrayList<String>(Arrays.asList(A2)),
                                                               new ArrayList<String>(Arrays.asList(A1)),
                                                               new ArrayList<String>(Arrays.asList(A0))};

        DPSearch forwardPath = coherentEntityLinker.dynamicProgrammingSearch(nBestList, surfaceStrings);
        DPSearch backwardPath = coherentEntityLinker.dynamicProgrammingSearch(nBestListReverse, coherentEntityLinker
            .reverseArray(surfaceStrings));

        String
            bestPath =
            coherentEntityLinker.bestMergedPath(forwardPath.logSimilarity, backwardPath.logSimilarity, nBestList);
        System.out.println(bestPath);

    }

    /**
     * Run forward-backward algorithm on multiple entity candidates and returns a filtered list of coherent entities
     * Takes nbest list and
     * @param nBestList an array of arraylists of wiki entities; for each entity span we have a list of candidate
     *                       wikilinks (nbest list)
     * @param entitySurfaceForms an array of entity spans
     * @return DPSearch object that contains lattice of paths and likelihood scores
     */
    public DPSearch dynamicProgrammingSearch(ArrayList<String>[] nBestList, String[] entitySurfaceForms) {

        int sequenceLength = nBestList.length;
        double[][] logSimilarityOverall = new double[sequenceLength][MAXNBEST];
        for (int i = 0; i < logSimilarityOverall.length; i++) {
            Arrays.fill(logSimilarityOverall[i], -10000.0);
        }
        String[][] path = new String[sequenceLength][MAXNBEST];
        for (int i = 0; i < path.length; i++) {
            Arrays.fill(path[i], "");
        }
        ListIterator<String> it = nBestList[0].listIterator();
        while (it.hasNext()) {
            //MAKE SURE to call NextIndex before Next

            int index = it.nextIndex();
            String currentCandidate = it.next();

            logSimilarityOverall[0][index] =
                Math.log((1 - LEXSIM_LAMBDA) * gensimEntityEmbeddings.entity2WordSimilarity(prependWiki(currentCandidate),

                                                                                 entitySurfaceForms[0]
                                                                                     .replace(" ", "_")) +

                         LEXSIM_LAMBDA * gensimEntityEmbeddings
                             .lexicalSimilarity(currentCandidate.replace("_", " "), entitySurfaceForms[0]));
            path[0][index] = currentCandidate;

        }
        System.out.println(Arrays.toString(path[0]));

        ListIterator<String> currentCandidateIterator, previousCandidateIterator;
        for (int i = 1; i < sequenceLength; i++) {
            currentCandidateIterator = nBestList[i].listIterator();

            while (currentCandidateIterator.hasNext()) {

                //MAKE SURE to call NextIndex before Next
                int currentCandidateIndex = currentCandidateIterator.nextIndex();

                String currentCandidate = currentCandidateIterator.next();

                //System.out.println(i + " , " + currentCandidateIndex);
                double
                    candidateNBestSimilarity =
                    Math.log((1 - LEXSIM_LAMBDA) * gensimEntityEmbeddings.entity2WordSimilarity(prependWiki(currentCandidate),
                                                                                     entitySurfaceForms[i]
                                                                                         .replace(" ", "_")) +
                             LEXSIM_LAMBDA * gensimEntityEmbeddings
                                 .lexicalSimilarity(currentCandidate.replace("_", " "), entitySurfaceForms[i]));

                double bestSimilarity = 0.0;
                double interCandidateSimilarity = 0.0;
                int previousBestCandidateIndex = -1;
                previousCandidateIterator = nBestList[i - 1].listIterator();
                while (previousCandidateIterator.hasNext()) {

                    //MAKE SURE to call NextIndex before Next

                    int index = previousCandidateIterator.nextIndex();
                    String previousCandidate = previousCandidateIterator.next();

                    interCandidateSimilarity =
                        Math.log((1 - LEXSIM_LAMBDA) * gensimEntityEmbeddings.entity2EntitySimilarity(prependWiki(previousCandidate),
                                                                                         prependWiki(currentCandidate))
                                 +
                                 LEXSIM_LAMBDA * gensimEntityEmbeddings
                                     .lexicalSimilarity(previousCandidate.replace("_", " "),
                                                        currentCandidate.replace("_", " ")));
                    if (bestSimilarity == 0.0) {
                        bestSimilarity = interCandidateSimilarity + logSimilarityOverall[i - 1][index];
                        previousBestCandidateIndex = index;

                    } else if (interCandidateSimilarity + logSimilarityOverall[i - 1][index] > bestSimilarity) {
                        bestSimilarity = interCandidateSimilarity + logSimilarityOverall[i - 1][index];
                        previousBestCandidateIndex = index;

                    }
                }
                try {
                    logSimilarityOverall[i][currentCandidateIndex] = bestSimilarity + candidateNBestSimilarity;

                    path[i][currentCandidateIndex] =
                        path[i - 1][previousBestCandidateIndex] + "," + currentCandidate;

                } catch (ArrayIndexOutOfBoundsException e) {
                    e.getMessage();
                }
            }


        }
        RealVector realVector = new ArrayRealVector(logSimilarityOverall[sequenceLength - 1]);
        int bestPathIndex = realVector.getMaxIndex();

        DPSearch dpSearch = new DPSearch(logSimilarityOverall, path);
        return dpSearch;
    }

    /**
     *
     * @param nBestList an array of arraylists of wiki entities; for each entity span we have a list of candidate
     *                       wikilinks (nbest list)
     * @param surfaceStrings  an array of entity spans
     * @return String of coherent entities
     */
    public String bestForwardPath(ArrayList<String>[] nBestList, String[] surfaceStrings) {
        DPSearch forwardPath = dynamicProgrammingSearch(nBestList, surfaceStrings);
        RealVector realVector = new ArrayRealVector(forwardPath.logSimilarity[surfaceStrings.length - 1]);
        int bestPathIndex = realVector.getMaxIndex();
        return forwardPath.path[surfaceStrings.length - 1][bestPathIndex];
    }

    /**
     * Same as bestForwardPath but runs over entities in reverse direction
     * @param nBestList an array of arraylists of wiki entities; for each entity span we have a list of candidate
     *                       wikilinks (nbest list)
     * @param surfaceStrings  an array of entity spans
     * @return String of coherent entities
     */
    public String bestBackwardPath(ArrayList<String>[] nBestList, String[] surfaceStrings) {
        for (int i = 0; i < nBestList.length / 2; i++) {
            ArrayList<String> temp = nBestList[i];
            nBestList[i] = nBestList[nBestList.length - i - 1];
            nBestList[nBestList.length - i - 1] = temp;

            String tempStr = surfaceStrings[i];
            surfaceStrings[i] = surfaceStrings[surfaceStrings.length - i - 1];
            surfaceStrings[surfaceStrings.length - i - 1] = tempStr;
        }
        return bestForwardPath(nBestList, surfaceStrings);
    }

    /**
     *
     * @param forwardPath lattice of forwardPath
     * @param backwardPath lattice of backwardPath
     * @param nBestList array of arraylists (of string type) with n-best list for each entity span
     * @return best forward + best backward path
     */
    public String bestMergedPath(double[][] forwardPath, double[][] backwardPath, ArrayList<String>[] nBestList) {
        int length = forwardPath.length;
        for (int i = 0; i < forwardPath.length; i++) {
            for (int j = 0; j < nBestList[i].size(); j++) {
                System.out
                    .println(forwardPath[i][j] + ", " + backwardPath[length - 1 - i][j] + ": " + nBestList[i].get(j));

                forwardPath[i][j] += backwardPath[length - 1 - i][j];
            }
        }
        StringBuilder bestPath = new StringBuilder();
        for (int i = 0; i < forwardPath.length; i++) {
            RealVector realVector = new ArrayRealVector(forwardPath[i]);
            int bestPathIndex = realVector.getMaxIndex();
            //nBestList[i].get(bestPathIndex);
            bestPath.append(nBestList[i].get(bestPathIndex));
            bestPath.append(",");
        }
        System.out.println(bestPath.toString());
        return bestPath.toString();

    }

    /**
     *
     * @param entity
     * @return entity with wiki label
     */
    public String prependWiki(String entity) {
        return "_wiki_" + entity;
    }

    /**
     *
     * @param surfaceStrings an array of entity spans
     * @return reversed array of surfaceStrings
     */
    public String[] reverseArray(String[] surfaceStrings) {

        for (int i = 0; i < surfaceStrings.length / 2; i++) {
            String temp = surfaceStrings[i];
            surfaceStrings[i] = surfaceStrings[surfaceStrings.length - i - 1];
            surfaceStrings[surfaceStrings.length - i - 1] = temp;
        }
        return surfaceStrings;
    }

}
