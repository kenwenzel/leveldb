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

import static org.iq80.leveldb.util.SizeOf.SIZE_OF_INT;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Comparator;
import java.util.NoSuchElementException;

import org.iq80.leveldb.impl.SeekingIterator;
import org.iq80.leveldb.table.BlockEntry;
import org.iq80.leveldb.util.Slice;
import org.iq80.leveldb.util.SliceInput;
import org.iq80.leveldb.util.SliceOutput;
import org.iq80.leveldb.util.Slices;
import org.iq80.leveldb.util.VariableLengthQuantity;
import org.iq80.leveldb.util.fpc.FpcCompressor;

import com.google.common.base.Preconditions;

public class TSBlockIterator implements SeekingIterator<Slice, Slice> {
    private final SliceInput data;
    private final Slice restartPositions;
    private final int restartCount;
    private final Comparator<Slice> comparator;

    private BlockEntry nextEntry;
    private Slice nextValue;

    protected FpcCompressor doubleCompressor;
    protected final double[] decodeBuffer = new double[2];
    protected final ByteBuffer bb;

    public TSBlockIterator(Slice data, Slice restartPositions, Comparator<Slice> comparator) {
	Preconditions.checkNotNull(data, "data is null");
	Preconditions.checkNotNull(restartPositions, "restartPositions is null");
	Preconditions.checkArgument(restartPositions.length() % SIZE_OF_INT == 0,
		"restartPositions.readableBytes() must be a multiple of %s", SIZE_OF_INT);
	Preconditions.checkNotNull(comparator, "comparator is null");

	this.data = data.input();
	this.bb = data.toByteBuffer();

	this.restartPositions = restartPositions.slice();
	restartCount = this.restartPositions.length() / SIZE_OF_INT;

	this.comparator = comparator;

	seekToFirst();
    }

    protected FpcCompressor getDoubleCompressor() {
	if (doubleCompressor == null) {
	    doubleCompressor = new FpcCompressor(2048);
	}
	return doubleCompressor;
    }

    @Override
    public boolean hasNext() {
	return nextEntry != null;
    }

    @Override
    public BlockEntry peek() {
	if (!hasNext()) {
	    throw new NoSuchElementException();
	}
	return nextEntry;
    }

    @Override
    public BlockEntry next() {
	if (!hasNext()) {
	    throw new NoSuchElementException();
	}

	BlockEntry entry = nextEntry;

	if (!data.isReadable()) {
	    nextEntry = null;
	} else {
	    // read entry at current data position
	    nextEntry = readEntry(data, nextEntry);
	}

	return entry;
    }

    @Override
    public void remove() {
	throw new UnsupportedOperationException();
    }

    /**
     * Repositions the iterator to the beginning of this block.
     */
    @Override
    public void seekToFirst() {
	if (restartCount > 0) {
	    seekToRestartPosition(0);
	}
    }

    /**
     * Repositions the iterator so the key of the next BlockElement returned
     * greater than or equal to the specified targetKey.
     */
    @Override
    public void seek(Slice targetKey) {
	if (restartCount == 0) {
	    return;
	}

	int left = 0;
	int right = restartCount - 1;

	// binary search restart positions to find the restart position
	// immediately before the targetKey
	while (left < right) {
	    int mid = (left + right + 1) / 2;

	    seekToRestartPosition(mid);

	    if (comparator.compare(nextEntry.getKey(), targetKey) < 0) {
		// key at mid is smaller than targetKey. Therefore all restart
		// blocks before mid are uninteresting.
		left = mid;
	    } else {
		// key at mid is greater than or equal to targetKey. Therefore
		// all restart blocks at or after mid are uninteresting.
		right = mid - 1;
	    }
	}

	// linear search (within restart block) for first key greater than or
	// equal to targetKey
	for (seekToRestartPosition(left); nextEntry != null; next()) {
	    if (comparator.compare(peek().getKey(), targetKey) >= 0) {
		break;
	    }
	}

    }

    /**
     * Seeks to and reads the entry at the specified restart position.
     * <p/>
     * After this method, nextEntry will contain the next entry to return, and
     * the previousEntry will be null.
     */
    private void seekToRestartPosition(int restartPosition) {
	Preconditions.checkPositionIndex(restartPosition, restartCount, "restartPosition");

	// seek data readIndex to the beginning of the restart block
	int offset = restartPositions.getInt(restartPosition * SIZE_OF_INT);
	data.setPosition(offset);

	// clear the entries to assure key is not prefixed
	nextEntry = null;
	nextValue = null;
	if (doubleCompressor != null) {
	    doubleCompressor.reset();
	}

	// read the entry
	nextEntry = readEntry(data, null);
    }

    /**
     * Reads the entry at the current data readIndex. After this method, data
     * readIndex is positioned at the beginning of the next entry or at the end
     * of data if there was not a next entry.
     *
     * @return true if an entry was read
     */
    private BlockEntry readEntry(SliceInput data, BlockEntry previousEntry) {
	Preconditions.checkNotNull(data, "data is null");

	// read entry header
	int sharedKeyLength = VariableLengthQuantity.readVariableLengthInt(data);
	int nonSharedKeyLength = VariableLengthQuantity.readVariableLengthInt(data);
	int valueLength;

	// read key
	Slice key = Slices.allocate(sharedKeyLength + nonSharedKeyLength);
	SliceOutput sliceOutput = key.output();
	if (sharedKeyLength > 0) {
	    Preconditions.checkState(previousEntry != null,
		    "Entry has a shared key but no previous entry was provided");
	    sliceOutput.writeBytes(previousEntry.getKey(), 0, sharedKeyLength);
	}
	sliceOutput.writeBytes(data, nonSharedKeyLength);

	// read value
	Slice value;
	int startPos = data.position();
	byte header = data.readByte();
	// use lower 7 bits as header
	char valueType = (char) (header & 0x7F);
	boolean variableLength = false;

	// value with fixed length
	switch (valueType) {
	case 'B':
	    valueLength = java.lang.Byte.BYTES;
	    break;
	case 'C':
	    valueLength = java.lang.Character.BYTES;
	    break;
	case 'd':
	    // compressed double value encoded in previous entry
	    valueLength = 0;
	    break;
	case 'D':
	    int compressionHeader = data.readByte();
	    valueLength = FpcCompressor.size(compressionHeader);
	    // reset position of input stream
	    data.setPosition(startPos + 1);
	    break;
	case 'F':
	    valueLength = java.lang.Float.BYTES;
	    break;
	case 'I':
	    valueLength = java.lang.Integer.BYTES;
	    break;
	case 'J':
	    valueLength = java.lang.Long.BYTES;
	    break;
	case 'S':
	    valueLength = java.lang.Short.BYTES;
	    break;
	case 'Z':
	    valueLength = java.lang.Byte.BYTES;
	    break;
	default:
	    // string or unknown value with variable length
	    valueLength = VariableLengthQuantity.readVariableLengthInt(data);
	    variableLength = true;
	}

	switch (valueType) {
	// use delta compression for double values
	case 'd':
	case 'D':
	    if (nextValue != null) {
		// TODO check why this is necessary
		data.setPosition(startPos + 1 + valueLength);
		value = nextValue;
		nextValue = null;
	    } else {
		bb.position(data.position());
		getDoubleCompressor().decode(bb, decodeBuffer, 0);
		byte[] firstDouble = ByteBuffer.allocate(1 + Double.BYTES).order(ByteOrder.BIG_ENDIAN).put((byte) 'D')
			.putDouble(decodeBuffer[0]).array();
		value = new Slice(firstDouble);
		boolean isSingleValue = (header & TSBlock.HEADER_LAST_VALUE) != 0;
		if (isSingleValue) {
		    nextValue = null;
		    doubleCompressor.reset();
		} else {
		    byte[] secondDouble = ByteBuffer.allocate(1 + Double.BYTES).order(ByteOrder.BIG_ENDIAN)
			    .put((byte) 'D').putDouble(decodeBuffer[1]).array();
		    nextValue = new Slice(secondDouble);
		}
		// update position of data stream
		data.setPosition(bb.position());
	    }
	    break;
	default:
	    // TODO maybe improve the encoding and add header after length to avoid copying the data
	    if (variableLength) {
		Slice valueSlice = data.readSlice(valueLength);
		byte[] rawData = valueSlice.getRawArray();
		byte[] newSlice = new byte[valueSlice.length() + 1];
		System.arraycopy(rawData, valueSlice.getRawOffset(), newSlice, 1, valueSlice.length());
		newSlice[0] = (byte) valueType;
		value = new Slice(newSlice);
	    } else {
		data.setPosition(startPos);
		value = data.readSlice(1 + valueLength);
	    }
	    break;
	}

	return new BlockEntry(key, value);
    }
}
