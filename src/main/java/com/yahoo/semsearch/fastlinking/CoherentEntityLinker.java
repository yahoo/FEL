/**
 Copyright 2016, Yahoo Inc.
 Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 **/
package com.yahoo.semsearch.fastlinking;

import com.yahoo.semsearch.fastlinking.w2v.CompressedSingleFileEntitySimilarityUtil;

import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealVector;

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

 java -Xmx512m -Xmx10g exec:java -Dexec.mainClass=com.yahoo.semsearch.fastlinking.CoherentEntityLinker -Dexec.args="enwiki.wiki2vec.d300.compressed"  -Dexec.classpathScope=compile
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
    // in final result string, delimiter between wiki candidates.
    // chosen from illegal characters in wiki ids: https://en.wikipedia.org/wiki/Wikipedia:Page_name#Spaces.2C_underscores_and_character_coding
    public static String CANDIDATE_DELIMITER = "#";

    public static double DEFAULT_LOG_LIKELIHOOD =  -10000.0;

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

    /**
     * @param entityEmbeddings - the absolute path of the compressed entity embeddings file
     */
    public CoherentEntityLinker(String entityEmbeddings) {
        try {
            gensimEntityEmbeddings = new CompressedSingleFileEntitySimilarityUtil(entityEmbeddings);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    @SuppressWarnings("unchecked")
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

        CoherentEntityLinker coherentEntityLinker = new CoherentEntityLinker(args[0]);

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
        
        /*
        // Print bestForwardPath and bestBackwardPath
        String bestForwardPath = coherentEntityLinker.bestForwardPath(nBestList, surfaceStrings);
        System.out.println("\nbestForwardPath: " + bestForwardPath);
        
        String bestBackwardPath = coherentEntityLinker.bestBackwardPath(nBestList, surfaceStrings);
        System.out.println("\nbestBackwardPath: " + bestBackwardPath);
        */

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
    public DPSearch dynamicProgrammingSearch(List<String>[] nBestList, String[] entitySurfaceForms) {

        int sequenceLength = entitySurfaceForms.length;
        int nBestLength = MAXNBEST;
        double[][] logSimilarityOverall = new double[sequenceLength][nBestLength];
        for (int i = 0; i < logSimilarityOverall.length; i++) {
            Arrays.fill(logSimilarityOverall[i], DEFAULT_LOG_LIKELIHOOD);
        }
        String[][] path = new String[sequenceLength][nBestLength];
        for (int i = 0; i < path.length; i++) {
            Arrays.fill(path[i], "");
        }
        ListIterator<String> it = nBestList[0].listIterator();
        while (it.hasNext()) {
            //MAKE SURE to call NextIndex before Next

            int index = it.nextIndex();
            String currentCandidate = it.next();

            double entity2WordSim = gensimEntityEmbeddings.entity2WordSimilarity(prependWiki(currentCandidate),
                                                                                 entitySurfaceForms[0]
                                                                                     .replace(" ", "_"));
            double lexicalSim = gensimEntityEmbeddings
                .lexicalSimilarity(currentCandidate.replace("_", " "), entitySurfaceForms[0]);

            logSimilarityOverall[0][index] =
                Math.max(Math.log((1 - LEXSIM_LAMBDA) * entity2WordSim +
                                  LEXSIM_LAMBDA * lexicalSim),DEFAULT_LOG_LIKELIHOOD);

            path[0][index] = currentCandidate;
        }

        ListIterator<String> currentCandidateIterator, previousCandidateIterator;
        for (int i = 1; i < sequenceLength; i++) {
            currentCandidateIterator = nBestList[i].listIterator();

            while (currentCandidateIterator.hasNext()) {

                //MAKE SURE to call NextIndex before Next
                int currentCandidateIndex = currentCandidateIterator.nextIndex();
                String currentCandidate = currentCandidateIterator.next();

                double entity2WordSim = gensimEntityEmbeddings.entity2WordSimilarity(prependWiki(currentCandidate),
                                                                                     entitySurfaceForms[i]
                                                                                         .replace(" ", "_"));
                double lexicalSim = gensimEntityEmbeddings
                    .lexicalSimilarity(currentCandidate.replace("_", " "), entitySurfaceForms[i]);

                double
                    candidateNBestSimilarity =
                    Math.log((1 - LEXSIM_LAMBDA) * entity2WordSim +
                             LEXSIM_LAMBDA * lexicalSim);

                double bestSimilarity = 0.0;
                double interCandidateSimilarity = 0.0;
                int previousBestCandidateIndex = -1;
                previousCandidateIterator = nBestList[i - 1].listIterator();
                while (previousCandidateIterator.hasNext()) {

                    //MAKE SURE to call NextIndex before Next

                    int index = previousCandidateIterator.nextIndex();
                    String previousCandidate = previousCandidateIterator.next();

                    double entity2EntitySimilarity = gensimEntityEmbeddings.entity2EntitySimilarity(prependWiki(previousCandidate),
                                                                                                    prependWiki(currentCandidate));

                    double entity2EntityLexicalSimilarity = gensimEntityEmbeddings
                        .lexicalSimilarity(previousCandidate.replace("_", " "),
                                           currentCandidate.replace("_", " "));

                    double jointSimilarity = (1 - LEXSIM_LAMBDA) * entity2EntitySimilarity + LEXSIM_LAMBDA * entity2EntityLexicalSimilarity;
                    interCandidateSimilarity = Math.log(jointSimilarity);


                    if (bestSimilarity == 0.0) {
                        bestSimilarity = interCandidateSimilarity + logSimilarityOverall[i - 1][index];
                        previousBestCandidateIndex = index;

                    } else if (interCandidateSimilarity + logSimilarityOverall[i - 1][index] > bestSimilarity) {
                        bestSimilarity = interCandidateSimilarity + logSimilarityOverall[i - 1][index];
                        previousBestCandidateIndex = index;

                    }
                }
                try {
                    logSimilarityOverall[i][currentCandidateIndex] = Math.max(bestSimilarity + candidateNBestSimilarity, DEFAULT_LOG_LIKELIHOOD);

                    path[i][currentCandidateIndex] =
                        path[i - 1][previousBestCandidateIndex] + CANDIDATE_DELIMITER + currentCandidate;

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
    public String bestForwardPath(List<String>[] nBestList, String[] surfaceStrings) {
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
    public String bestBackwardPath(List<String>[] nBestList, String[] surfaceStrings) {
        for (int i = 0; i < nBestList.length / 2; i++) {
            List<String> temp = nBestList[i];
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
    public String bestMergedPath(double[][] forwardPath, double[][] backwardPath, List<String>[] nBestList) {
        int length = forwardPath.length;
        for (int i = 0; i < forwardPath.length; i++) {
            for (int j = 0; j < nBestList[i].size(); j++) {
                //System.out.println(forwardPath[i][j] + ", " + backwardPath[length - 1 - i][j] + ": " + nBestList[i].get(j));

                forwardPath[i][j] += backwardPath[length - 1 - i][j];
            }
        }
        StringBuilder bestPath = new StringBuilder();
        for (int i = 0; i < forwardPath.length; i++) {
            RealVector realVector = new ArrayRealVector(forwardPath[i]);
            int bestPathIndex = realVector.getMaxIndex();
            bestPath.append(nBestList[i].get(bestPathIndex));
            bestPath.append(CANDIDATE_DELIMITER);
        }
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
