/**
 Copyright 2016, Yahoo Inc.
 Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 **/

package com.yahoo.semsearch.fastlinking.view;

/**
 * Holder for a pair (surface form, info)
 * surface form is represented with a String and the information about the alias with a @see CandidatesInfo object
 *
 * @author roi blanco
 */
public class StringAndCandidate {
    public CandidatesInfo candidatesInfo;
    public String surfaceForm;
}