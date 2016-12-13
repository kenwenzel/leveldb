/*
 * Copyright (C) 2011 the original author or authors.
 * See the notice.md file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.iq80.leveldb.util.fpc;

import java.nio.ByteBuffer;

public class FpcCompressor {
	private static final int logOfTableSize = 16;
	FcmPredictor predictor1 = new FcmPredictor(logOfTableSize);
	DfcmPredictor predictor2 = new DfcmPredictor(logOfTableSize);

	public void compress(ByteBuffer buff, double[] doubles) {
		for (int i = 0; i < doubles.length; i += 2) {
			if (i == doubles.length - 1) {
				encodeAndPad(buff, doubles[i]);
			} else {
				encode(buff, doubles[i], doubles[i + 1]);
			}
		}
	}

	public void decompress(ByteBuffer buff, double[] dest) {
		for (int i = 0; i < dest.length; i += 2) {
			decode(buff, dest, i);
		}
	}

	public void decode(ByteBuffer buff, double[] dest, int i) {
		byte header = buff.get();

		long prediction;

		if ((header & 0x80) != 0) {
			prediction = predictor2.getPrediction();
		} else {
			prediction = predictor1.getPrediction();
		}

		int numZeroBytes = (header & 0x70) >> 4;
		if (numZeroBytes > 3) {
			numZeroBytes++;
		}
		byte[] dst = new byte[8 - numZeroBytes];
		buff.get(dst);
		long diff = toLong(dst);
		long actual = prediction ^ diff;

		predictor1.update(actual);
		predictor2.update(actual);

		dest[i] = Double.longBitsToDouble(actual);

		if ((header & 0x08) != 0) {
			prediction = predictor2.getPrediction();
		} else {
			prediction = predictor1.getPrediction();
		}

		numZeroBytes = (header & 0x07);
		if (numZeroBytes > 3) {
			numZeroBytes++;
		}
		dst = new byte[8 - numZeroBytes];
		buff.get(dst);
		diff = toLong(dst);

		if (numZeroBytes == 7 && diff == 0) {
			return;
		}
		actual = prediction ^ diff;

		predictor1.update(actual);
		predictor2.update(actual);

		dest[i + 1] = Double.longBitsToDouble(actual);
	}

	public long toLong(byte[] dst) {
		long result = 0L;
		for (int i = dst.length; i > 0; i--) {
			result = result << 8;
			result |= dst[i - 1] & 0xff;
		}
		return result;
	}

	public void encodeAndPad(ByteBuffer buf, double d) {
		long dBits = Double.doubleToLongBits(d);
		long diff1d = predictor1.getPrediction() ^ dBits;
		long diff2d = predictor2.getPrediction() ^ dBits;

		boolean predictor1BetterForD = Long.numberOfLeadingZeros(diff1d) >= Long.numberOfLeadingZeros(diff2d);

		predictor1.update(dBits);
		predictor2.update(dBits);

		byte code = 0;
		if (predictor1BetterForD) {
			int zeroBytes = encodeZeroBytes(diff1d);
			code |= zeroBytes << 4;
		} else {
			code |= 0x80;
			int zeroBytes = encodeZeroBytes(diff2d);
			code |= zeroBytes << 4;
		}

		code |= 0x06;

		buf.put(code);
		if (predictor1BetterForD) {
			buf.put(toByteArray(diff1d));
		} else {
			buf.put(toByteArray(diff2d));
		}

		buf.put((byte) 0);

	}

	private int encodeZeroBytes(long diff1d) {
		int leadingZeroBytes = Long.numberOfLeadingZeros(diff1d) / 8;
		if (leadingZeroBytes >= 4) {
			leadingZeroBytes--;
		}
		return leadingZeroBytes;
	}

	public void encode(ByteBuffer buf, double prev, double next) {
		long prevBits = Double.doubleToLongBits(prev);
		long diff1d = predictor1.getPrediction() ^ prevBits;
		long diff2d = predictor2.getPrediction() ^ prevBits;

		boolean predictor1BetterForD = Long.numberOfLeadingZeros(diff1d) >= Long.numberOfLeadingZeros(diff2d);

		predictor1.update(prevBits);
		predictor2.update(prevBits);

		long nextBits = Double.doubleToLongBits(next);
		long diff1e = predictor1.getPrediction() ^ nextBits;
		long diff2e = predictor2.getPrediction() ^ nextBits;

		boolean predictor1BetterForE = Long.numberOfLeadingZeros(diff1e) >= Long.numberOfLeadingZeros(diff2e);

		predictor1.update(nextBits);
		predictor2.update(nextBits);

		byte code = 0;
		if (predictor1BetterForD) {
			int zeroBytes = encodeZeroBytes(diff1d);
			code |= zeroBytes << 4;
		} else {
			code |= 0x80;
			int zeroBytes = encodeZeroBytes(diff2d);
			code |= zeroBytes << 4;
		}

		if (predictor1BetterForE) {
			int zeroBytes = encodeZeroBytes(diff1e);
			code |= zeroBytes;
		} else {
			code |= 0x08;
			int zeroBytes = encodeZeroBytes(diff2e);
			code |= zeroBytes;
		}

		buf.put(code);
		if (predictor1BetterForD) {
			buf.put(toByteArray(diff1d));
		} else {
			buf.put(toByteArray(diff2d));
		}

		if (predictor1BetterForE) {
			buf.put(toByteArray(diff1e));
		} else {
			buf.put(toByteArray(diff2e));
		}
	}

	public byte[] toByteArray(long diff) {
		int encodedZeroBytes = encodeZeroBytes(diff);
		if (encodedZeroBytes > 3) {
			encodedZeroBytes++;
		}
		byte[] array = new byte[8 - encodedZeroBytes];
		for (int i = 0; i < array.length; i++) {
			array[i] = (byte) (diff & 0xff);
			diff = diff >> 8;
		}
		return array;
	}
	
	public void reset() {
	    predictor1.reset();
	    predictor2.reset();
	}
}
