package it.cnr.isti.hpc;

public class CentroidEntityScorer extends EntityScorer {

	public CentroidEntityScorer(Word2VecCompress word_model,
			Word2VecCompress entity_model) {
		super(word_model, entity_model);

		if (entity_model.dimensions() != word_model.dimensions()) {
			throw new IllegalArgumentException(
					"Word and entity models have incompatible vector dimensions");
		}
	}
	
	public class CentroidScorerContext extends ScorerContext {
		float[] centroid_vec;
		float norm;
		public CentroidScorerContext(float[] word_vecs, int[] word_counts) {
			super(word_vecs, word_counts);
			// compute context centroid
			int word_size = word_model.dimensions();
			int n_words = word_counts.length;
			centroid_vec = new float[word_size];
			for (int i = 0; i < n_words; ++i) {
				int word_count = word_counts[i];
				for (int j = 0; j < word_size; ++j) {
					centroid_vec[j] += word_count * word_vecs[i * word_size + j];
				}
			}

			norm = LinearAlgebra.inner(centroid_vec.length, centroid_vec, 0, centroid_vec, 0);
			norm = (float) Math.sqrt(norm);
		}
		
		@Override
		float compute_score() {
			int word_size = centroid_vec.length;
			return LinearAlgebra.inner(word_size, entity_vec, 0, centroid_vec, 0) / norm;
		}
		
	}

	@Override
	ScorerContext create_context(float[] word_vecs, int[] word_counts) {
		return new CentroidScorerContext(word_vecs, word_counts);
	}

}
