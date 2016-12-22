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
    private final File databaseDir = FileUtils.createTempDir("leveldb");

    public static byte[] bytes(long value) {
	return ByteBuffer.allocate(Long.BYTES).order(ByteOrder.BIG_ENDIAN).putLong(value).array();
    }

    public static long inv(long value) {
	return Long.MAX_VALUE - value;
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
	options.timeSeriesMode(true);

	File path = getTestDirectory("testTimeSeries");
	DB db = factory.open(path, options);

	Random rnd = new Random(200);
	long startTime = 1478252048736L;

	double[] template = { 0.0, 0.0123, 0.0532324, 0.02, 0.03344, 0.13, 0.03 };

	System.out.println("Adding");
	for (int i = 0; i < 1000 * 1000; i++) {
	    if (i % 10000 == 0) {
		System.out.println("  at: " + i);
	    }

	    long currentTime = startTime + i * 100;
	    double value = template[rnd.nextInt(template.length)];
	    byte[] valueBytes = ByteBuffer.allocate(1 + Double.BYTES).order(ByteOrder.BIG_ENDIAN).put((byte) 'D')
		    .putDouble(value).array();

	    db.put(bytes(currentTime), valueBytes);
	}

	db.close();
	db = factory.open(path, options);

	System.out.println("Reading");
	// restart random numbers
	rnd = new Random(200);

	DBIterator it = db.iterator();
	for (int i = 0; i < 10 * 1000; i++) {
	    Entry<byte[], byte[]> entry = it.next();

	    long currentTime = startTime + i * 100;
	    double value = template[rnd.nextInt(template.length)];
	    byte[] valueBytes = ByteBuffer.allocate(1 + Double.BYTES).order(ByteOrder.BIG_ENDIAN).put((byte) 'D')
		    .putDouble(value).array();

	    assertEquals(entry.getKey(), bytes(currentTime));
	    assertEquals(entry.getValue(), valueBytes);
	}
	it.close();
	db.close();

	db = factory.open(path, options);
	System.out.println("Read random");

	for (int count = 0; count < 100; count++) {
	    // restart random numbers
	    rnd = new Random(200);

	    int start = rnd.nextInt(200);
	    int i = 0;
	    while (i++ < start) {
		// consume random value
		rnd.nextInt(template.length);
	    }

	    it = db.iterator();
	    it.seek(bytes(startTime + i * 100));
	    for (; i - start < 10; i++) {
		Entry<byte[], byte[]> entry = it.next();

		long currentTime = startTime + i * 100;
		double value = template[rnd.nextInt(template.length)];
		byte[] valueBytes = ByteBuffer.allocate(1 + Double.BYTES).order(ByteOrder.BIG_ENDIAN).put((byte) 'D')
			.putDouble(value).array();

		assertEquals(entry.getKey(), bytes(currentTime));
		assertEquals(entry.getValue(), valueBytes);
	    }
	    it.close();
	}
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
	options.timeSeriesMode(true);
	options.reverseOrdering(true);

	File path = getTestDirectory("testTimeSeriesReverse");
	DB db = factory.open(path, options);

	Random rnd = new Random(200);
	long startTime = 1478252048736L;

	double[] template = { 0.0, 0.0123, 0.0532324, 0.02, 0.03344, 0.13, 0.03 };

	int nrOfValues = 1000 * 1000;

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
	    double value = template[rnd.nextInt(template.length)];
	    byte[] valueBytes = ByteBuffer.allocate(1 + Double.BYTES).order(ByteOrder.BIG_ENDIAN).put((byte) 'D')
		    .putDouble(value).array();

	    if (rnd2.nextDouble() < 0.2) {
		toAdd = 100;
	    }
	    if (toAdd-- > 0) {
		recordedEntries.add(new Pair<>(bytes(currentTime), valueBytes));
		if (toAdd == 0) {
		    recordedEntries.add(null);
		}
	    }

	    db.put(bytes(currentTime), valueBytes);
	}

	// reverse the recorded entries since values
	Collections.reverse(recordedEntries);

	while (recordedEntries.get(0) == null) {
	    recordedEntries.remove(0);
	}

	db.close();
	db = factory.open(path, options);

	System.out.println("Reading reverse");
	DBIterator it = db.iterator();
	boolean seek = false;
	for (Pair<byte[], byte[]> testEntry : recordedEntries) {
	    if (testEntry == null) {
		seek = true;
	    } else {
		if (seek) {
		    it.seek(testEntry.key);
		}

		Entry<byte[], byte[]> dbEntry = it.next();
		assertEquals(dbEntry.getKey(), testEntry.key);
		assertEquals(dbEntry.getValue(), testEntry.value);
	    }
	}

	db.close();
    }
}
