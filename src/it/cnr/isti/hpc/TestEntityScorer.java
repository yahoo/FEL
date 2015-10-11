package it.cnr.isti.hpc;

import it.unimi.dsi.fastutil.io.BinIO;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TestEntityScorer {
	
	static volatile float barrier;

	public static void main(String[] args) throws Exception {
		String scorer_type = args[0];
		
		Word2VecCompress word_model = (Word2VecCompress)BinIO.loadObject(args[1]);
		Word2VecCompress entity_model = (Word2VecCompress)BinIO.loadObject(args[2]);
		boolean perf = false;
		if (args.length > 3 && args[3].equals("--perf")) {
			perf = true;
		}
		EntityScorer scorer;
		if ("lr".equals(scorer_type)) {
			scorer = new LREntityScorer(word_model, entity_model);
		} else if ("centroid".equals(scorer_type)) {
			scorer = new CentroidEntityScorer(word_model, entity_model);
		} else if ("nop".equals(scorer_type)) {
			scorer = new NopEntityScorer(word_model, entity_model);
		} else {
			throw new Exception("Unknown scorer type");
		}
				
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		while (true) {
			if (!perf) System.out.println("Entities:");
			List<String> entities = Arrays.asList(br.readLine().split(" "));
			if (!perf) System.out.println("Context:");
			List<String> words = Arrays.asList(br.readLine().split(" "));

			List<Long> entity_ids = new ArrayList<>();
			for (String entity: entities) {
				entity_ids.add(entity_model.word_id(entity));
			}
			
			if (!perf) {
				float[] scores = scorer.score_ids(entity_ids, words);
				for (int e = 0; e < entities.size(); ++e) {
					System.out.printf("%s: %f\n", entities.get(e), scores[e]);
				}
			} else {
				int runs = 1000;
				float[] scores = null;
				long tick = System.nanoTime();
				for (int run = 0; run < runs; ++run) {
					scores = scorer.score_ids(entity_ids, words);
					barrier = scores[0];
				}
				long elapsed = System.nanoTime() - tick;
				double us_per_query = elapsed / 1000. / runs;
				System.out.printf("%f us per query\n", us_per_query);
							
				for (int e = 0; e < Math.min(10, scores.length); ++e) {
					System.out.printf("%f\t", scores[e]);
				}
				System.out.println();
			}
		}
	}

}
