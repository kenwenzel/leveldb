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

    protected ByteBuffer encodingBuffer = ByteBuffer.allocate(64);

    public TSBlockBuilder(int estimatedSize, int blockRestartInterval, Comparator<Slice> comparator) {
	super(estimatedSize, blockRestartInterval, comparator);
    }

    public void reset() {
	super.reset();
	doubleCompressor = null;
    }

    protected void writeRaw(Slice key, Slice value) {
	Preconditions.checkState(!finished, "block is finished");
	Preconditions.checkPositionIndex(restartBlockEntryCount, blockRestartInterval);

	Preconditions.checkArgument(lastKey == null || comparator.compare(key, lastKey) > 0,
		"key must be greater than last key");
	
	int sharedKeyBytes = 0;
	if (restartBlockEntryCount < blockRestartInterval) {
	    sharedKeyBytes = calculateSharedBytes(key, lastKey);
	} else {
	    // restart prefix and value compression
	    restartPositions.add(block.size());
	    restartBlockEntryCount = 0;
	    doubleCompressor = null;
	}

	int nonSharedKeyBytes = key.length() - sharedKeyBytes;

	// write "<shared><non_shared><value_size>"
	VariableLengthQuantity.writeVariableLengthInt(sharedKeyBytes, block);
	VariableLengthQuantity.writeVariableLengthInt(nonSharedKeyBytes, block);
	VariableLengthQuantity.writeVariableLengthInt(value.length(), block);

	// write non-shared key bytes
	block.writeBytes(key, sharedKeyBytes, nonSharedKeyBytes);

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
	    double prev = ByteBuffer.wrap(holdValue.getRawArray()).order(ByteOrder.BIG_ENDIAN).getDouble(1);
	    encodingBuffer.clear();
	    getDoubleCompressor().encodeAndPad(encodingBuffer, prev);
	    Slice newValue = new Slice(encodingBuffer.array(), 0, encodingBuffer.position());
	    writeRaw(holdKey, newValue);
	    holdKey = null;
	    holdValue = null;
	}
    }

    protected FpcCompressor getDoubleCompressor() {
	FpcCompressor comp = doubleCompressor;
	if (comp == null) {
	    comp = doubleCompressor = new FpcCompressor();
	}
	return comp;
    }

    public void add(Slice key, Slice value) {
	Preconditions.checkNotNull(key, "key is null");
	Preconditions.checkNotNull(value, "value is null");

	if (restartBlockEntryCount >= blockRestartInterval) {
	    // this is a restart position
	    // ensure that value compression starts again
	    writePrevValue();
	}
	
	int valueLength = value.length();
	if (valueLength > 0) {
	    switch ((char) value.getByte(0)) {
	    // use delta compression for double values
	    case 'D':
		if (holdKey != null) {
		    double prev = ByteBuffer.wrap(holdValue.getRawArray()).order(ByteOrder.BIG_ENDIAN).getDouble(1);
		    double next = ByteBuffer.wrap(value.getRawArray()).order(ByteOrder.BIG_ENDIAN).getDouble(1);
		    encodingBuffer.clear();
		    getDoubleCompressor().encode(encodingBuffer, prev, next);
		    Slice newValue = new Slice(encodingBuffer.array(), 0, encodingBuffer.position());
		    writeRaw(holdKey, newValue);
		    writeRaw(key, new Slice(value.getRawArray(), 0, 1));
		    holdKey = null;
		    holdValue = null;
		} else {
		    holdKey = key;
		    holdValue = value;
		}
		break;
	    default:
		writePrevValue();
		writeRaw(key, value);
	    }
	} else {
	    writeRaw(key, value);
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