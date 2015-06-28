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
package org.iq80.leveldb.table;

import com.google.common.base.Preconditions;

import org.iq80.leveldb.DBBufferComparator;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.impl.DbImplTest.StrictMemoryManager;
import org.iq80.leveldb.impl.InternalKey;
import org.iq80.leveldb.impl.InternalKeyComparator;
import org.iq80.leveldb.impl.ReverseSeekingIterator;
import org.iq80.leveldb.impl.TransientInternalKey;
import org.iq80.leveldb.impl.ValueType;
import org.iq80.leveldb.util.Closeables;
import org.iq80.leveldb.util.Snappy;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;

import static java.util.Arrays.asList;
import static org.testng.Assert.assertTrue;

public abstract class TableTest
{
    private File file;
    private RandomAccessFile randomAccessFile;
    private FileChannel fileChannel;
    private static final DBBufferComparator byteCompare = new BytewiseComparator();

    protected abstract Table createTable(String name, FileChannel fileChannel, Options options)
            throws IOException;

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testEmptyFile()
            throws Exception
    {
        createTable(file.getAbsolutePath(), fileChannel,
                Options.make().bufferComparator(byteCompare).verifyChecksums(true).compression(Snappy.instance())).close();
    }

    @Test
    public void testEmptyBlock()
            throws Exception
    {
        tableTest(Integer.MAX_VALUE, Integer.MAX_VALUE);
    }

    @Test
    public void testSingleEntrySingleBlock()
            throws Exception
    {
        tableTest(Integer.MAX_VALUE, Integer.MAX_VALUE, BlockHelper.createInternalEntry("name", "dain sundstrom", 0));
    }

    @Test
    public void testMultipleEntriesWithSingleBlock()
            throws Exception
    {
        long seq = 0;
        List<Entry<InternalKey, ByteBuffer>> entries = asList(
                BlockHelper.createInternalEntry("beer/ale", "Lagunitas  Little Sumpin’ Sumpin’", seq++),
                BlockHelper.createInternalEntry("beer/ipa", "Lagunitas IPA", seq++),
                BlockHelper.createInternalEntry("beer/stout", "Lagunitas Imperial Stout", seq++),
                BlockHelper.createInternalEntry("scotch/light", "Oban 14", seq++),
                BlockHelper.createInternalEntry("scotch/medium", "Highland Park", seq++),
                BlockHelper.createInternalEntry("scotch/strong", "Lagavulin", seq++));

        for (int i = 1; i < entries.size(); i++) {
            tableTest(Integer.MAX_VALUE, i, entries);
        }
    }

    @Test
    public void testMultipleEntriesWithMultipleBlock()
            throws Exception
    {
        long seq = 0;
        List<Entry<InternalKey, ByteBuffer>> entries = asList(
                BlockHelper.createInternalEntry("beer/ale", "Lagunitas  Little Sumpin’ Sumpin’", seq++),
                BlockHelper.createInternalEntry("beer/ipa", "Lagunitas IPA", seq++),
                BlockHelper.createInternalEntry("beer/stout", "Lagunitas Imperial Stout", seq++),
                BlockHelper.createInternalEntry("scotch/light", "Oban 14", seq++),
                BlockHelper.createInternalEntry("scotch/medium", "Highland Park", seq++),
                BlockHelper.createInternalEntry("scotch/strong", "Lagavulin", seq++));

        // one entry per block
        tableTest(1, Integer.MAX_VALUE, entries);

        // about 3 blocks
        tableTest(BlockHelper.estimateBlockSizeInternalKey(Integer.MAX_VALUE, entries) / 3, Integer.MAX_VALUE, entries);
    }

    @SafeVarargs
    private final void tableTest(int blockSize, int blockRestartInterval, Entry<InternalKey, ByteBuffer>... entries)
            throws IOException
    {
        tableTest(blockSize, blockRestartInterval, asList(entries));
    }

    private void tableTest(int blockSize, int blockRestartInterval, List<Entry<InternalKey, ByteBuffer>> entries)
            throws IOException
    {
        List<Entry<InternalKey, ByteBuffer>> reverseEntries = new ArrayList<>(entries);
        Collections.reverse(reverseEntries);

        reopenFile();
        try (StrictMemoryManager strictMemory = new StrictMemoryManager()) {
            Options options = Options.make()
                    .blockSize(blockSize)
                    .blockRestartInterval(blockRestartInterval)
                    .memoryManager(strictMemory)
                    .compression(Snappy.instance())
                    .bufferComparator(byteCompare);

            try (TableBuilder builder = new TableBuilder(options, fileChannel, new InternalKeyComparator(
                    options.bufferComparator()))) {
                for (Entry<InternalKey, ByteBuffer> entry : entries) {
                    builder.add(entry.getKey(), entry.getValue());
                }
                builder.finish();
            }

            try (Table table = createTable(file.getAbsolutePath(), fileChannel, options);
                    TableIterator tableIter = table.retain().iterator()) {
                ReverseSeekingIterator<InternalKey, ByteBuffer> seekingIterator = tableIter;

                seekingIterator.seekToFirst();
                BlockHelper.assertReverseSequence(seekingIterator,
                        Collections.<Entry<InternalKey, ByteBuffer>> emptyList());
                BlockHelper.assertSequence(seekingIterator, entries);
                BlockHelper.assertReverseSequence(seekingIterator, reverseEntries);

                seekingIterator.seekToEnd();
                BlockHelper.assertSequence(seekingIterator, Collections.<Entry<InternalKey, ByteBuffer>> emptyList());
                BlockHelper.assertReverseSequence(seekingIterator, reverseEntries);
                BlockHelper.assertSequence(seekingIterator, entries);

                long lastApproximateOffset = 0;
                for (Entry<InternalKey, ByteBuffer> entry : entries) {
                    List<Entry<InternalKey, ByteBuffer>> nextEntries = entries.subList(entries.indexOf(entry),
                            entries.size());
                    seekingIterator.seek(entry.getKey());
                    BlockHelper.assertSequence(seekingIterator, nextEntries);

                    seekingIterator.seek(BlockHelper.beforeInternalKey(entry));
                    BlockHelper.assertSequence(seekingIterator, nextEntries);

                    seekingIterator.seek(BlockHelper.afterInternalKey(entry));
                    BlockHelper.assertSequence(seekingIterator, nextEntries.subList(1, nextEntries.size()));

                    long approximateOffset = table.getApproximateOffsetOf(entry.getKey());
                    assertTrue(approximateOffset >= lastApproximateOffset);
                    lastApproximateOffset = approximateOffset;
                }

                InternalKey endKey = new TransientInternalKey(ByteBuffer.wrap(new byte[] { (byte) 0xFF, (byte) 0xFF,
                        (byte) 0xFF, (byte) 0xFF }), 0, ValueType.VALUE);
                seekingIterator.seek(endKey);
                BlockHelper.assertSequence(seekingIterator, Collections.<BlockEntry<InternalKey>> emptyList());
                BlockHelper.assertReverseSequence(seekingIterator, reverseEntries);

                long approximateOffset = table.getApproximateOffsetOf(endKey);
                assertTrue(approximateOffset >= lastApproximateOffset);
            }
        }
    }

    @BeforeMethod
    public void setUp()
            throws Exception
    {
        reopenFile();
        Preconditions.checkState(0 == fileChannel.position(), "Expected fileChannel.position %s to be 0", fileChannel.position());
    }

    private void reopenFile()
            throws IOException
    {
        file = File.createTempFile("table", ".db");
        file.delete();
        randomAccessFile = new RandomAccessFile(file, "rw");
        fileChannel = randomAccessFile.getChannel();
    }

    @AfterMethod
    public void tearDown()
            throws Exception
    {
        Closeables.closeQuietly(fileChannel);
        Closeables.closeQuietly(randomAccessFile);
        file.delete();
    }

    public static class FileChannelTableTest
            extends TableTest
    {
        @Override
        protected Table createTable(String name, FileChannel fileChannel, Options options)
                throws IOException
        {
            return new FileChannelTable(name, fileChannel, new InternalKeyComparator(options.bufferComparator()),
                    options);
        }
    }

    public static class MMapTableTest
            extends TableTest
    {
        @Override
        protected Table createTable(String name, FileChannel fileChannel, Options options)
                throws IOException
        {
            return new MMapTable(name, fileChannel, new InternalKeyComparator(options.bufferComparator()), options);
        }
    }
}
