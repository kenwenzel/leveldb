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
import java.util.Map.Entry;
import java.util.Random;

import org.iq80.leveldb.CompressionType;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBException;
import org.iq80.leveldb.DBFactory;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.impl.Iq80DBFactory;
import org.iq80.leveldb.util.FileUtils;
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
    public void testTimeSeries() throws IOException, DBException {
	Options options = new Options().createIfMissing(true).compressionType(CompressionType.NONE)
		.blockRestartInterval(500);
	options.timeSeriesMode(true);

	File path = getTestDirectory("testTimeSeries");
	DB db = factory.open(path, options);

	Random rnd = new Random(200);
	long startTime = 1478252048736L;

	double[] template = { 0.0, 0.0123, 0.0532324, 0.02, 0.03344, 0.13, 0.03 };

	System.out.println("Adding");
	for (int i = 0; i < 500 * 1000; i++) {
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

	db.close();
    }
}
