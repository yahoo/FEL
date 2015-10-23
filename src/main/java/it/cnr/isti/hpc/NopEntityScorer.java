package it.cnr.isti.hpc;


public class NopEntityScorer extends EntityScorer {

	public NopEntityScorer(Word2VecCompress word_model, Word2VecCompress entity_model) {
		super(word_model, entity_model);
	}

	public class NopScorerContext extends ScorerContext {
		public NopScorerContext(float[] word_vecs, int[] word_counts) {
			super(word_vecs, word_counts);
		}
		
		@Override
		float compute_score() {
			return 0;
		}
	}
	
	@Override
	ScorerContext create_context(float[] word_vecs, int[] word_counts) {
		return new NopScorerContext(word_vecs, word_counts);
	}

}
