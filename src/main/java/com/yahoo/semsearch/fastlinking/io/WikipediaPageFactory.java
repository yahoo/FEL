

/*
 * Cloud9: A MapReduce Library for Hadoop
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.yahoo.semsearch.fastlinking.io;

import edu.umd.cloud9.collection.wikipedia.WikipediaPage;
import edu.umd.cloud9.collection.wikipedia.language.ArabicWikipediaPage;
import edu.umd.cloud9.collection.wikipedia.language.ChineseWikipediaPage;
import edu.umd.cloud9.collection.wikipedia.language.CzechWikipediaPage;
import edu.umd.cloud9.collection.wikipedia.language.GermanWikipediaPage;
import edu.umd.cloud9.collection.wikipedia.language.SpanishWikipediaPage;
import edu.umd.cloud9.collection.wikipedia.language.SwedishWikipediaPage;
import edu.umd.cloud9.collection.wikipedia.language.TurkishWikipediaPage;

/**
 * Hadoop {@code WikipediaPageFactory} for creating language dependent WikipediaPage Objects. From https://lintool.github.io/Cloud9/
 *
 * @author Peter Exner
 * @author Ferhan Ture
 */
public class WikipediaPageFactory {

	/**
	 * Returns a {@code WikipediaPage} for this {@code language}.
	 */
	public static WikipediaPage createWikipediaPage(String language) {
		if (language == null) {
			return new EnglishWikipediaPage();
		}

		if (language.equalsIgnoreCase("en")) {
			return new EnglishWikipediaPage();
		} else if (language.equalsIgnoreCase("sv")) {
			return new SwedishWikipediaPage();
		} else if (language.equalsIgnoreCase("de")) {
			return new GermanWikipediaPage();
		} else if (language.equalsIgnoreCase("cs")) {
			return new CzechWikipediaPage();
		} else if (language.equalsIgnoreCase("es")) {
			return new SpanishWikipediaPage();
		} else if (language.equalsIgnoreCase("ar")) {
			return new ArabicWikipediaPage();
		} else if (language.equalsIgnoreCase("tr")) {
			return new TurkishWikipediaPage();
		} else if (language.equalsIgnoreCase("zh")) {
			return new ChineseWikipediaPage();
		} else {
			return new EnglishWikipediaPage();
		}
	}

	public static Class<? extends WikipediaPage> getWikipediaPageClass(String language) {
		if (language == null) {
			return EnglishWikipediaPage.class;
		}

		if (language.equalsIgnoreCase("en")) {
			return EnglishWikipediaPage.class;
		} else if (language.equalsIgnoreCase("sv")) {
			return SwedishWikipediaPage.class;
		} else if (language.equalsIgnoreCase("de")) {
			return GermanWikipediaPage.class;
		} else {
			return EnglishWikipediaPage.class;
		}
	}
}
