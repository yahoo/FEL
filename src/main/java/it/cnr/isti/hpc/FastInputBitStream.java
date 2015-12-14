package it.cnr.isti.hpc;

import it.unimi.dsi.bits.Fast;
import it.unimi.dsi.fastutil.io.FastByteArrayOutputStream;
import it.unimi.dsi.io.OutputBitStream;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Random;

public class FastInputBitStream {

	private ByteBuffer buf;
	private long current;
	private int fill;

	public FastInputBitStream(byte[] a) {
		buf = ByteBuffer.wrap(a);
		buf.order(ByteOrder.BIG_ENDIAN); // should be default anyway
		fill = 0;
		current = 0;
	}

	void refill() {
		assert fill <= 32;
		current = (current << 32) | (buf.getInt() & 0xFFFFFFFFL);
		fill += 32;
	}

	public void position(long pos) {
		int wordPos = (int)(pos / 32);
		buf.position(wordPos * 4); // we need to preserve the alignment
		current = buf.getInt() & 0xFFFFFFFFL;
		fill = 32 - (int)(pos % 32);
	}
	
	public int readInt(int len) {
		if (len > fill) {
			refill();
		}
		assert len <= fill;
		return (int) (current >>> (fill -= len)) & ((1 << len) - 1);
	}

	public int readUnary() {
		int x = 0;

		// this optimization is wrong if we're at the end of the stream, 
		// commenting out for now
//		if (fill < 32) {
//			refill();
//		}
		
		while (true) {
			int z = Long.numberOfLeadingZeros(current << (64 - fill));
			if (z < fill) { // This works also when fill = 0
				fill -= z + 1;
				return x + z;
			}
			x += fill;
			current = buf.getInt() & 0xFFFFFFFFL;
			fill = 32;
		}
	}
	
	public int readMinimalBinary( final int b ) {
		return readMinimalBinary( b, Fast.mostSignificantBit( b ) );
	}

	public int readMinimalBinary( final int b, final int log2b ) {
		if ( b < 1 ) throw new IllegalArgumentException( "The bound " + b + " is not positive" );

		final int m = ( 1 << log2b + 1 ) - b; 
		final int x = readInt( log2b );

		if ( x < m ) return x;
		else return ( ( x << 1 ) + readInt(1) - m );
	}


	public int readGolomb( final int b ) {
		return readGolomb( b, Fast.mostSignificantBit( b ) );
	}

	public int readGolomb( final int b, final int log2b ) {
		if ( b < 0 ) throw new IllegalArgumentException( "The modulus " + b + " is negative" );
		if ( b == 0 ) return 0;

		return readUnary() * b + readMinimalBinary( b, log2b );
	}

	public static void main(String[] args) throws IOException {
		Random rng = new Random(1729);
		// test readInt
		{
			int n = (1 << 16) + 1;
			int[] bs = new int[n];
			int[] values = new int[n];
			FastByteArrayOutputStream oa = new FastByteArrayOutputStream();
			OutputBitStream obs = new OutputBitStream(oa, 0);

			for (int i = 0; i < n; ++i) {
				bs[i] = rng.nextInt(31); // XXX 33?
				values[i] = rng.nextInt(1 << bs[i]);
				obs.writeInt(values[i], bs[i]);
			}

			obs.close();
			while (oa.length() % 4 != 0)
				oa.write(0);
			oa.trim();

			FastInputBitStream ibs = new FastInputBitStream(oa.array);
			int pos = 0;
			for (int i = 0; i < n; ++i) {
				long val = ibs.readInt(bs[i]);
				if (val != values[i]) {
					System.out.printf(
							"i=%d, b=%d, pos=%d, expected=%d, got=%d\n", i,
							bs[i], pos, values[i], val);
					System.exit(1);
				}
				pos += bs[i];
			}
			
			for (int i = n - 1; i >= 0; --i) {
				pos -= bs[i];
				ibs.position(pos);
				long val = ibs.readInt(bs[i]);
				if (val != values[i]) {
					System.out.printf(
							"i=%d, b=%d, pos=%d, expected=%d, got=%d\n", i,
							bs[i], pos, values[i], val);
					System.exit(1);
				}
			}
		}

		// test readUnary
		{
			int n = (1 << 16) + 1;
			int[] values = new int[n];
			FastByteArrayOutputStream oa = new FastByteArrayOutputStream();
			OutputBitStream obs = new OutputBitStream(oa, 0);

			for (int i = 0; i < n; ++i) {
				values[i] = rng.nextInt(256);
				obs.writeUnary(values[i]);
			}

			obs.close();
			while (oa.length() % 4 != 0)
				oa.write(0);
			oa.trim();

			FastInputBitStream ibs = new FastInputBitStream(oa.array);
			for (int i = 0; i < n; ++i) {
				long val = ibs.readUnary();;
				if (val != values[i]) {
					System.out.printf(
							"i=%d, expected=%d, got=%d\n", i,
							values[i], val);
					System.exit(1);
				}
			}
		}
		
		// test readGolomb
		{
			int n = (1 << 16) + 1;
			int[] bs = new int[n];
			int[] values = new int[n];
			FastByteArrayOutputStream oa = new FastByteArrayOutputStream();
			OutputBitStream obs = new OutputBitStream(oa, 0);

			for (int i = 0; i < n; ++i) {
				bs[i] = rng.nextInt(1024) + 1;
				values[i] = rng.nextInt(128) * bs[i] + rng.nextInt(bs[i]);
				obs.writeGolomb(values[i], bs[i]);
			}

			obs.close();
			while (oa.length() % 4 != 0)
				oa.write(0);
			oa.trim();

			FastInputBitStream ibs = new FastInputBitStream(oa.array);
			for (int i = 0; i < n; ++i) {
				long val = ibs.readGolomb(bs[i]);
				if (val != values[i]) {
					System.out.printf(
							"i=%d, b=%d, expected=%d, got=%d\n", i,
							bs[i], values[i], val);
					System.exit(1);
				}
			}
		}

		
		System.out.println("Testing done");
	}

}
