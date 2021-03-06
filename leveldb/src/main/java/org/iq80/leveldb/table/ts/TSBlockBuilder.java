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
package org.iq80.leveldb.table.ts;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Comparator;

import org.iq80.leveldb.table.BlockBuilder;
import org.iq80.leveldb.util.Slice;
import org.iq80.leveldb.util.VariableLengthQuantity;
import org.iq80.leveldb.util.fpc.FpcCompressor;

import com.google.common.base.Preconditions;

/**
 * A block build that uses delta-compression techniques especially suited for
 * time series data.
 */
public class TSBlockBuilder extends BlockBuilder {
    protected FpcCompressor doubleCompressor;

    // this key and the value cannot be written before the next value is known
    protected Slice holdKey, holdValue;

    protected ByteBuffer encodingBuffer = ByteBuffer.allocate(Double.BYTES * 4);

    // true, if the compression should be restarted with the next call of
    // writeRaw
    protected boolean restartCompression = false;

    public TSBlockBuilder(int estimatedSize, int blockRestartInterval, Comparator<Slice> comparator) {
	super(estimatedSize, blockRestartInterval, comparator);
    }

    public void reset() {
	super.reset();
	if (doubleCompressor != null) {
	    doubleCompressor.reset();
	}
    }

    protected void writeRaw(Slice key, int header, Slice value) {
	Preconditions.checkState(!finished, "block is finished");
	// Preconditions.checkPositionIndex(restartBlockEntryCount,
	// blockRestartInterval);

	Preconditions.checkArgument(lastKey == null || comparator.compare(key, lastKey) > 0,
		"key must be greater than last key");

	int sharedKeyBytes = 0;
	if (restartCompression) {
	    restartCompression = false;

	    // restart prefix and value compression
	    // this is done here to allow writeRaw to write two successive
	    // values for double compression and others
	    restartPositions.add(block.size());
	    restartBlockEntryCount = 0;
	    if (doubleCompressor != null) {
		doubleCompressor.reset();
	    }
	} else {
	    sharedKeyBytes = calculateSharedBytes(key, lastKey);
	}

	int nonSharedKeyBytes = key.length() - sharedKeyBytes;

	// write "<shared><non_shared><value_size>"
	VariableLengthQuantity.writeVariableLengthInt(sharedKeyBytes, block);
	VariableLengthQuantity.writeVariableLengthInt(nonSharedKeyBytes, block);
	// value length is only written for certain variable-length values

	// write non-shared key bytes
	block.writeBytes(key, sharedKeyBytes, nonSharedKeyBytes);

	// write value header
	block.writeByte(header);

	// write value length if required
	if ((header & TSBlock.HEADER_VALUE_LENGTH_ENCODED) != 0) {
	    VariableLengthQuantity.writeVariableLengthInt(value.length(), block);
	}

	// write value bytes
	block.writeBytes(value, 0, value.length());

	// update last key
	lastKey = key;

	// update state
	entryCount++;
	restartBlockEntryCount++;
    }

    protected void writePrevValue() {
	if (holdKey != null) {
	    ByteBuffer bb = holdValue.toByteBuffer().order(ByteOrder.BIG_ENDIAN);
	    // skip marker
	    bb.get();
	    double prev = bb.getDouble();
	    encodingBuffer.clear();
	    getDoubleCompressor().encodeAndPad(encodingBuffer, prev);
	    Slice newValue = new Slice(encodingBuffer.array(), 0, encodingBuffer.position());
	    writeRaw(holdKey, encodeHeader('D', true), newValue);
	    holdKey = null;
	    holdValue = null;
	}
    }

    protected FpcCompressor getDoubleCompressor() {
	if (doubleCompressor == null) {
	    doubleCompressor = new FpcCompressor(2048);
	}
	return doubleCompressor;
    }

    protected int encodeHeader(char valueType, boolean isLast) {
	// uses lower 8 bits for type encoding and the upper bits (> 8) for
	// flags which are not written to the output stream
	int header = (byte) valueType;
	switch (valueType) {
	case 'd':
	case 'D':
	    if (isLast) {
		// is last value
		header |= TSBlock.HEADER_LAST_VALUE;
	    }
	    break;
	case 'B':
	case 'C':
	case 'F':
	case 'I':
	case 'J':
	case 'S':
	case 'Z':
	    break;
	default:
	    // encode length
	    header |= TSBlock.HEADER_VALUE_LENGTH_ENCODED;
	    break;
	}
	return header;
    }

    public void add(Slice key, Slice value) {
	Preconditions.checkNotNull(key, "key is null");
	Preconditions.checkNotNull(value, "value is null");

	if (restartBlockEntryCount >= blockRestartInterval) {
	    // ensure that last value is written
	    writePrevValue();

	    restartCompression = true;
	}

	int valueLength = value.length();
	if (valueLength > 0) {
	    char valueType = (char) value.getByte(0);
	    switch (valueType) {
	    // use optimized delta compression for double values
	    case 'D':
		if (holdKey != null) {
		    ByteBuffer bb = holdValue.toByteBuffer().order(ByteOrder.BIG_ENDIAN);
		    // skip marker
		    bb.get();
		    double prev = bb.getDouble();
		    bb = value.toByteBuffer().order(ByteOrder.BIG_ENDIAN);
		    // skip marker
		    bb.get();
		    double next = bb.getDouble();
		    encodingBuffer.clear();
		    getDoubleCompressor().encode(encodingBuffer, prev, next);

		    Slice newValue = new Slice(encodingBuffer.array(), 0, encodingBuffer.position());
		    writeRaw(holdKey, encodeHeader('D', false), newValue);
		    // write only 'D' as indicator for a double value
		    writeRaw(key, encodeHeader('d', false), new Slice(new byte[0]));
		    holdKey = null;
		    holdValue = null;
		} else {
		    holdKey = key;
		    holdValue = value;
		}
		break;
	    default:
		writePrevValue();
		writeRaw(key, encodeHeader(valueType, false), value.slice(1, value.length() - 1));
		break;
	    }
	} else {
	    writePrevValue();
	    writeRaw(key, encodeHeader('N', false), value);
	}
    }

    @Override
    public Slice finish() {
	if (!finished) {
	    // ensure that pending double value is written
	    writePrevValue();
	}
	return super.finish();
    }
}