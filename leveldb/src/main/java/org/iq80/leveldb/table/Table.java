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
import com.google.common.base.Throwables;

import org.iq80.leveldb.MemoryManager;
import org.iq80.leveldb.impl.SeekingIterable;
import org.iq80.leveldb.util.Closeables;
import org.iq80.leveldb.util.TableIterator;
import org.iq80.leveldb.util.VariableLengthQuantity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Comparator;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class Table
        implements SeekingIterable<ByteBuffer, ByteBuffer>, AutoCloseable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(Table.class);
    protected final String name;
    protected final FileChannel fileChannel;
    protected final Comparator<ByteBuffer> comparator;
    protected final boolean verifyChecksums;
    protected final Block indexBlock;
    protected final BlockHandle metaindexBlockHandle;
    protected final MemoryManager memory;
    private final AtomicInteger refCount;

    public Table(String name,
            FileChannel fileChannel,
            Comparator<ByteBuffer> comparator,
            boolean verifyChecksums,
            MemoryManager memory)
            throws IOException
    {
        Preconditions.checkNotNull(name, "name is null");
        Preconditions.checkNotNull(fileChannel, "fileChannel is null");
        long size = fileChannel.size();
        Preconditions.checkArgument(size >= Footer.ENCODED_LENGTH, "File is corrupt: size must be at least %s bytes", Footer.ENCODED_LENGTH);
        Preconditions.checkArgument(size <= Integer.MAX_VALUE, "File must be smaller than %s bytes", Integer.MAX_VALUE);
        Preconditions.checkNotNull(comparator, "comparator is null");

        this.name = name;
        this.fileChannel = fileChannel;
        this.verifyChecksums = verifyChecksums;
        this.comparator = comparator;

        Footer footer = init();
        indexBlock = readBlock(footer.getIndexBlockHandle());
        metaindexBlockHandle = footer.getMetaindexBlockHandle();
        this.refCount = new AtomicInteger(1);
        this.memory = memory;
    }

    protected abstract Footer init()
            throws IOException;

    @Override
    public TableIterator iterator()
    {
        return new TableIterator(this, indexBlock.iterator());
    }

    public Block openBlock(ByteBuffer blockEntry)
    {
        BlockHandle blockHandle = BlockHandle.readBlockHandle(blockEntry);
        Block dataBlock;
        try {
            dataBlock = readBlock(blockHandle);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
        return dataBlock;
    }

    // TODO FIXME
    protected static ByteBuffer uncompressedScratch = ByteBuffer.allocateDirect(4 * 1024 * 1024);

    protected abstract Block readBlock(BlockHandle blockHandle)
            throws IOException;

    protected int uncompressedLength(ByteBuffer data)
    {
        int length = VariableLengthQuantity.readVariableLengthInt(data.duplicate());
        return length;
    }

    /**
     * Given a key, return an approximate byte offset in the file where
     * the data for that key begins (or would begin if the key were
     * present in the file).  The returned value is in terms of file
     * bytes, and so includes effects like compression of the underlying data.
     * For example, the approximate offset of the last key in the table will
     * be close to the file length.
     */
    public long getApproximateOffsetOf(ByteBuffer key)
    {
        BlockIterator iterator = indexBlock.iterator();
        iterator.seek(key);
        if (iterator.hasNext()) {
            BlockHandle blockHandle = BlockHandle.readBlockHandle(iterator.next().getValue());
            return blockHandle.getOffset();
        }

        // key is past the last key in the file.  Approximate the offset
        // by returning the offset of the metaindex block (which is
        // right near the end of the file).
        return metaindexBlockHandle.getOffset();
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("Table");
        sb.append("{name='").append(name).append('\'');
        sb.append(", comparator=").append(comparator);
        sb.append(", verifyChecksums=").append(verifyChecksums);
        sb.append('}');
        return sb.toString();
    }

    public Table retain()
    {
        int count;
        do {
            count = refCount.get();
            if (count == 0) {
                // raced with a final release,
                // force the caller to re-read from cache
                return null;
            }
        }
        while (!refCount.compareAndSet(count, count + 1));
        return this;
    }

    public void release()
    {
        if (refCount.decrementAndGet() == 0) {
            try {
                closer().call();
            }
            catch (Exception e) {
            }
        }
    }

    @Override
    public void close()
    {
        release();
    }

    @Override
    public void finalize()
            throws Throwable
    {
        if (refCount.get() != 0) {
            LOGGER.warn("table finalized with {} open refs", refCount.get());
        }
        try {
            closer().call();
        }
        catch (Exception e) {
            LOGGER.warn("exception in finalizing table", e);
            throw e;
        }
        finally {
            super.finalize();
        }
    }

    public Callable<?> closer()
    {
        return new Closer(fileChannel);
    }

    private static class Closer
            implements Callable<Void>
    {
        private final Closeable closeable;

        public Closer(Closeable closeable)
        {
            this.closeable = closeable;
        }

        @Override
        public Void call()
        {
            Closeables.closeQuietly(closeable);
            return null;
        }
    }
}
