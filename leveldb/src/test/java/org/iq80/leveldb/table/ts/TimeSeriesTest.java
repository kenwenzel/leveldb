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

import static org.testng.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;

import org.iq80.leveldb.CompressionType;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBException;
import org.iq80.leveldb.DBFactory;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.impl.InternalKey;
import org.iq80.leveldb.impl.Iq80DBFactory;
import org.iq80.leveldb.impl.TSInternalKeyFactory;
import org.iq80.leveldb.impl.ValueType;
import org.iq80.leveldb.util.FileUtils;
import org.iq80.leveldb.util.Slices;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Test the implementation of time series data.
 */
public class TimeSeriesTest {
    private static final boolean TIME_SERIES_MODE = true;

    private final File databaseDir = FileUtils.createTempDir("leveldb");

    public static byte[] bytes(long value) {
	return ByteBuffer.allocate(Long.BYTES).order(ByteOrder.BIG_ENDIAN).putLong(value).array();
    }

    public static long toLong(byte[] bytes) {
	return ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).getLong();
    }

    public static byte[] bytes(String value) {
	if (value == null) {
	    return null;
	}
	try {
	    return value.getBytes("UTF-8");
	} catch (UnsupportedEncodingException e) {
	    throw new RuntimeException(e);
	}
    }

    private final DBFactory factory = Iq80DBFactory.factory;

    File getTestDirectory(String name) throws IOException {
	File rc = new File(databaseDir, name);
	factory.destroy(rc, new Options().createIfMissing(true));
	rc.mkdirs();
	return rc;
    }

    @Test
    public void testKeys() {
	TSInternalKeyFactory f = new TSInternalKeyFactory();

	long startTime = 1478252048736L;
	for (int i = 0; i < 500 * 1000; i++) {
	    long currentTime = startTime + i * 100;

	    InternalKey key1 = f.createInternalKey(Slices.wrappedBuffer(bytes(currentTime)), i, ValueType.VALUE);
	    InternalKey key2 = f.createInternalKey(key1.encode());

	    Assert.assertEquals(key2, key1);
	}
    }

    @Test
    public void testTimeSeries() throws IOException, DBException {
	Options options = new Options().createIfMissing(true).compressionType(CompressionType.SNAPPY)
		.blockRestartInterval(500);
	options.timeSeriesMode(TIME_SERIES_MODE);

	File path = getTestDirectory("testTimeSeries");
	DB db = factory.open(path, options);

	Random rnd = new Random(200);
	long startTime = 1478252048736L;

	int nrOfValues = 1 * 1000 * 1000;

	// holds generated data
	List<Pair<byte[], byte[]>> recordedEntries = new ArrayList<>();

	int toAdd = 0;
	Random rnd2 = new Random(200);

	System.out.println("Adding");
	for (int i = 0; i < nrOfValues; i++) {
	    if (i % 10000 == 0) {
		System.out.println("  at: " + i);
	    }

	    long currentTime = startTime + i * 100;

	    byte[] keyBytes = bytes(currentTime);
	    double value = rnd.nextGaussian(); // Math.sin(i / 1000.0 *
					       // Math.PI);
	    byte[] valueBytes = ByteBuffer.allocate(1 + Double.BYTES).order(ByteOrder.BIG_ENDIAN).put((byte) 'D')
		    .putDouble(value).array();

	    if (toAdd-- > 0) {
		recordedEntries.add(new Pair<>(keyBytes, valueBytes));
		if (toAdd == 0) {
		    recordedEntries.add(null);
		}
	    } else if (rnd2.nextDouble() < 0.01) {
		toAdd = rnd2.nextInt(10);
	    }

	    db.put(keyBytes, valueBytes);
	}

	db.close();
	db = factory.open(path, options);

	System.out.println("Reading");
	readElements(recordedEntries, db.iterator());
	db.close();
    }

    private static class Pair<A, B> {
	final A key;
	final B value;

	Pair(A a, B b) {
	    this.key = a;
	    this.value = b;
	}
    }

    @Test
    public void testTimeSeriesReverse() throws IOException, DBException {
	Options options = new Options().createIfMissing(true).compressionType(CompressionType.SNAPPY)
		.blockRestartInterval(500);
	options.timeSeriesMode(TIME_SERIES_MODE);
	options.reverseOrdering(true);

	File path = getTestDirectory("testTimeSeriesReverse");
	DB db = factory.open(path, options);

	Random rnd = new Random(200);
	long startTime = 1478252048736L;

	int nrOfValues = 1 * 1000 * 1000;

	// holds generated data
	List<Pair<byte[], byte[]>> recordedEntries = new ArrayList<>();

	int toAdd = 0;
	Random rnd2 = new Random(200);

	System.out.println("Adding");
	for (int i = 0; i < nrOfValues; i++) {
	    if (i % 10000 == 0) {
		System.out.println("  at: " + i);
	    }

	    long currentTime = startTime + i * 100;
	    double value = Math.sin(i / 1000.0 * Math.PI);
	    byte[] valueBytes = ByteBuffer.allocate(1 + Double.BYTES).order(ByteOrder.BIG_ENDIAN).put((byte) 'D')
		    .putDouble(value).array();

	    if (toAdd-- > 0) {
		recordedEntries.add(new Pair<>(bytes(currentTime), valueBytes));
		if (toAdd == 0) {
		    recordedEntries.add(null);
		}
	    } else if (rnd2.nextDouble() < 0.01) {
		toAdd = rnd2.nextInt(10);
	    }

	    db.put(bytes(currentTime), valueBytes);
	}

	db.close();
	db = factory.open(path, options);

	System.out.println("Reading reverse");
	// reverse the recorded entries since values are stored in reverse order
	Collections.reverse(recordedEntries);
	readElements(recordedEntries, db.iterator());

	db.close();
    }

    @Test
    public void testRandomTime() throws Exception {
	Options options = new Options().createIfMissing(true).compressionType(CompressionType.SNAPPY)
		.blockRestartInterval(500);
	options.timeSeriesMode(TIME_SERIES_MODE);
	options.reverseOrdering(true);

	File path = getTestDirectory("testRandomTime");
	DB db = factory.open(path, options);

	Random rnd = new Random(200);
	long lastTime = 1478252048736L;

	int nrOfValues = 5 * 1000 * 1000;
	int nrOfSequentialValues = 100;
	int nrOfStartPoints = nrOfValues / nrOfSequentialValues;
	List<Long> timePoints = new ArrayList<>();
	for (int i = 0; i < nrOfStartPoints; i++) {
	    long currentTime = lastTime + nrOfSequentialValues + rnd.nextInt(10000);
	    timePoints.add(currentTime);
	    lastTime = currentTime;
	}

	Collections.shuffle(timePoints, rnd);

	// holds generated data
	List<Pair<byte[], byte[]>> recordedEntries = new ArrayList<>();

	int toAdd = 0;

	System.out.println("Adding random");
	int nr = 0;
	for (Long timePoint : timePoints) {
	    boolean added = false;

	    byte prefix = (byte) rnd.nextInt(100);

	    for (int i = 0; i < nrOfSequentialValues; i++) {
		if (nr % 10000 == 0) {
		    System.out.println("  at: " + nr);
		}

		long time = timePoint + i;

		ByteBuffer finalKey = ByteBuffer.allocate(1 + Long.BYTES).order(ByteOrder.BIG_ENDIAN);
		finalKey.put((byte) prefix);
		finalKey.putLong(1, time);
		byte[] keyBytes = finalKey.array();

		int value = rnd.nextInt();
		byte[] valueBytes;
		if (rnd.nextBoolean()) {
		    // long value
		    valueBytes = ByteBuffer.allocate(1 + Long.BYTES).order(ByteOrder.BIG_ENDIAN).put((byte) 'J')
			    .putLong(value).array();
		} else {
		    // some black-box object
		    valueBytes = new byte[8];
		    rnd.nextBytes(valueBytes);
		    valueBytes[0] = 'O';
		}

		if (toAdd-- > 0) {
		    recordedEntries.add(new Pair<>(keyBytes, valueBytes));
		    added = true;
		    if (toAdd == 0) {
			// add seek marker
			recordedEntries.add(null);
		    }
		} else if (rnd.nextDouble() < 0.01) {
		    toAdd = rnd.nextInt(10);
		}

		db.put(keyBytes, valueBytes);
		nr++;
	    }
	    if (added && recordedEntries.get(recordedEntries.size() - 1) != null) {
		// adding seek marker here works for forward and reverse
		// ordering
		recordedEntries.add(null);
	    }
	}
	// wait for compaction
//	Thread.sleep(10000);
	
	db.close();
	db = factory.open(path, options);

	System.out.println("Reading random");
	Collections.reverse(recordedEntries);
	readElements(recordedEntries, db.iterator());

	db.close();
    }

    String toString(byte[] key) {
	StringBuilder sb = new StringBuilder();
	int prefixLength = key.length - 8;
	for (int i = 0; i < prefixLength; i++) {
	    sb.append(key[i]);
	}
	if (sb.length() > 0) {
	    sb.append(" ");
	}
	long time = ByteBuffer.wrap(key, prefixLength, 8).order(ByteOrder.BIG_ENDIAN).getLong();
	sb.append(time);
	return sb.toString();
    }

    void readElements(List<Pair<byte[], byte[]>> expected, DBIterator it) {
	System.out.println("Read " + expected.size() + " elements.");
	boolean seek = true;
	for (Pair<byte[], byte[]> testEntry : expected) {
	    if (testEntry == null) {
		seek = true;
	    } else {
		if (seek) {
		    // System.out.println("seek");
		    it.seek(testEntry.key);
		    seek = false;
		}

		Entry<byte[], byte[]> dbEntry = it.next();
		// System.out.println(toString(dbEntry.getKey()) + " <--> " +
		// toString(testEntry.key));
		assertEquals(dbEntry.getKey(), testEntry.key);
		assertEquals(dbEntry.getValue(), testEntry.value);
	    }
	}
    }
}
