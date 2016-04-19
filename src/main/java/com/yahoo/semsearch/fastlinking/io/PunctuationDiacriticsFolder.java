/**
 Copyright 2016, Yahoo Inc.
 Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 **/

package com.yahoo.semsearch.fastlinking.io;

import java.text.Normalizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PunctuationDiacriticsFolder {

	public static final Pattern SPACE = Pattern.compile("[\\p{Space}\\p{Cntrl}]+");
	public static final Pattern PUNCT = Pattern.compile("\\p{Punct}+");

	public static final String SUBST_SPACE = Matcher.quoteReplacement(" ");
	public static final String SUBST_EMPTY = Matcher.quoteReplacement("");

	public static final Pattern DIACRITICS = Pattern.compile("[\\p{InCombiningDiacriticalMarks}\\p{IsLm}\\p{IsSk}]+");

	private static String stripDiacritics(String str) {
		str = Normalizer.normalize(str, Normalizer.Form.NFD);
		str = DIACRITICS.matcher(str).replaceAll(SUBST_EMPTY);
		return str;
	}

	public static String normalize(String str) {
		String norm = str;
		norm = PUNCT.matcher(norm).replaceAll(SUBST_SPACE);
		norm = SPACE.matcher(norm).replaceAll(SUBST_SPACE);
		norm = stripDiacritics(norm);

		return norm.toLowerCase().trim();
	}

	public static void main(String args[]) {
		System.out
		.println(PunctuationDiacriticsFolder
				.normalize("'''Jim Durham''' (February 12, '47 - November 4, 2012) was an [[United States of America|American]] [[sportscaster]]. Durham died on November 4, 2012.abc-def\nhello's"));
	}
}
