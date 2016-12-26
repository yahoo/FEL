/**
 Copyright 2016, Yahoo Inc.
 Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 **/
package com.yahoo.semsearch.fastlinking;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import com.google.common.collect.Iterators;
import com.yahoo.semsearch.fastlinking.utils.EntityLinkingUtils;

import static java.util.stream.Collectors.toList;

/**
 * Created by aasishkp on 08/25/16.
 * This class takes entity-mentions, entity embeddings, and entity hash datapack. It calls CoherentEntityLinker to generate N-best entity links with overall coherency!
 *
 * mvn clean compile  exec:java -Dexec.mainClass=com.yahoo.semsearch.fastlinking.CoherentEntityLinkerWrapper -Dexec.args="enwiki.wiki2vec.d300.compressed english-nov15.hash test.txt"  -Dexec.classpathScope=compile
 */
public class CoherentEntityLinkerWrapper {

    private final FastEntityLinker fastEntityLinker;
    private final CoherentEntityLinker coherentEntityLinker;
    public boolean useCoherentLinker = true;
    protected static final int NUM_CANDIDATES = 10;
    private int numCandidatesToRetrieve = NUM_CANDIDATES;

    public CoherentEntityLinkerWrapper(FastEntityLinker linker, CoherentEntityLinker coherentEntityLinker)
    {
        this.fastEntityLinker = linker;
        this.coherentEntityLinker = coherentEntityLinker;
    }

    public List<Optional<EntityResult>> linkTextsToEntities(List<String> rawTexts) {

        List<String> texts = rawTexts.stream().map(String::trim).collect(toList());

        // 1) Fetch candidates using helper method getWikiCandidates
        List<List<EntityResult>> allWikiCandidates = texts.stream()
            .map(text -> getWikiCandidates(text))
            .collect(toList());

        // 2) To filter from multiple candidates to the most likely wiki result,
        // either use CoherentEntityLinker or just keep the first candidate (with the highest score)
        return useCoherentLinker ?
               filterUsingCoherentLinker(allWikiCandidates, texts) : keepFirstCandidate(allWikiCandidates);
    }

    /**
     * 1) Get the candidate wikis for a given query, via FEL's {@link FastEntityLinker#getResultsGreedy(String, int)}
     * @param query - a span of text
     * @return an ordered list of wikis (best to worst) for that entity.
     */
    private List<EntityResult> getWikiCandidates(String query) {
        List<FastEntityLinker.EntityResult> felCandidates =
            fastEntityLinker.getResultsGreedy(query, this.numCandidatesToRetrieve);

        List<EntityResult> candidates = felCandidates.stream().map(felResult -> {
            String wikiId = EntityLinkingUtils.hexadecimalToChar(felResult.text.toString());
            return new EntityResult(wikiId, felResult.score, felResult.type);
        }).collect(toList());
        return candidates;
    }


    /**
     * 2) Filter multiple candidates to get the most likely result
     * @param wikiResults - for each entity, a list of candidates
     * @param texts - list of entity strings
     * @return for each entity, a single EntityResult (or optional)
     */
    @SuppressWarnings("unchecked")
    private List<Optional<EntityResult>> filterUsingCoherentLinker(List<List<EntityResult>> wikiResults, List<String> texts) {
        List<String> textsToLink = new ArrayList<>();
        List<List<String>> candidatesToLink = new ArrayList<>();
        for (int i = 0; i < texts.size(); i++) {
            List<EntityResult> candidates = wikiResults.get(i);
            if (!candidates.isEmpty()) {
                textsToLink.add(texts.get(i));
                candidatesToLink.add(candidates.stream().map(res -> res.wikiId).collect(toList()));
            }
        }

        if (textsToLink.isEmpty()) {
            return keepFirstCandidate(wikiResults);
        }

        // cast data structures into required types, and call CoherentEntityLinker.bestForwardPath
        List<String>[] nBestList = candidatesToLink.stream().toArray(List[]::new);
        String[] surfaceStrings = textsToLink.stream().toArray(String[]::new);
        String[] coherentResults = coherentEntityLinker.bestForwardPath(nBestList, surfaceStrings)
            .split(CoherentEntityLinker.CANDIDATE_DELIMITER);

        // if CoherentEntityLinker returns empty/odd output (due to unknown words), then fallback to 1-best algorithm
        if (coherentResults.length != surfaceStrings.length) {
            return keepFirstCandidate(wikiResults);
        }
        Iterator<String> coherentResultsIter = Iterators.forArray(coherentResults);

        // Use coherentResults to filter candidates
        List<Optional<EntityResult>> results = new ArrayList<>();
        for (List<EntityResult> candidates: wikiResults) {
            if (candidates.isEmpty()) {
                results.add(Optional.empty());
            } else {
                // filter candidates by only keeping the EntityResult whose id matches the best id found by the CoherentEntityLinker
                String bestWikiId = coherentResultsIter.next();
                Optional<EntityResult> bestCandidate = candidates.stream().filter(er -> bestWikiId.equals(er.wikiId)).findFirst();
                results.add(bestCandidate);
            }
        }
        return results;
    }

    /** "1-best" method: for each query, simply keep the first candidate of its list of candidates */
    private List<Optional<EntityResult>> keepFirstCandidate(List<List<EntityResult>> allCandidates) {
        return allCandidates.stream().map(EntityLinkingUtils::getFirstElement).collect(toList());
    }


    public static void main(String[] args){

        String[]
            namedEntityStrings =
            new String[]{"Huma Abedin", "Hillary Clinton", "Clinton", "Elijah Cummings", "Abedin", "Clinton"};


            try {
                CoherentEntityLinker coherentEntityLinker = new CoherentEntityLinker(args[0]);
                FastEntityLinker candidateRetriever = new FastEntityLinker(args[1]);
                List<String> entityStrings = EntityLinkingUtils.readEntityStrings(args[2]);

                if(entityStrings.isEmpty()){
                    entityStrings = Arrays.asList(namedEntityStrings);
                }

                CoherentEntityLinkerWrapper coherentEntityLinkerWrapper = new CoherentEntityLinkerWrapper(candidateRetriever,
                                                                                                          coherentEntityLinker);
                List<Optional<EntityResult>> entityResults = coherentEntityLinkerWrapper.linkTextsToEntities(entityStrings);

                for(Optional<EntityResult> res : entityResults){

                    System.out.println(res.get().wikiId);
                }
            }
            catch (ClassNotFoundException | IOException ex) {
                System.err.println("Couldn't initialize the linker");
            }
    }


    public static class EntityResult {
        public final String wikiId;
        public final double score;
        public final int type;

        public EntityResult(String wikiId, double score, int type) {
            this.wikiId = wikiId;
            this.score = score;
            this.type = type;
        }
    }
}
