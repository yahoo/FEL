package it.cnr.isti.hpc;

public class LinearAlgebra {
	public static float inner(int size, float[] v1, int offset1, float[] v2, int offset2) {
		if (size % 4 != 0) {
			throw new IllegalArgumentException("Vector size must be a multiple of 4");
		}
		
		float x0 = 0, x1 = 0, x2 = 0, x3 = 0;
		// manually unroll to help the compiler autovectorizer
		// (current JVM does not support vectorized accumulation)
		for (int i = 0; i < size; i += 4) {
			x0 += v1[offset1 + i + 0] * v2[offset2 + i + 0];
			x1 += v1[offset1 + i + 1] * v2[offset2 + i + 1];
			x2 += v1[offset1 + i + 2] * v2[offset2 + i + 2];
			x3 += v1[offset1 + i + 3] * v2[offset2 + i + 3];			
		}
		
		return x0 + x1 + x2 + x3;
	}
}
