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
import org.iq80.leveldb.util.ByteBuffers;
import org.iq80.leveldb.util.ReferenceCounted;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.Comparator;

import static org.iq80.leveldb.util.SizeOf.SIZE_OF_INT;

/**
 * Binary Structure
 * <table summary="record format">
 * <tbody>
 * <thead>
 * <tr>
 * <th>name</th>
 * <th>offset</th>
 * <th>length</th>
 * <th>description</th>
 * </tr>
 * </thead>
 * <p/>
 * <tr>
 * <td>entries</td>
 * <td>4</td>
 * <td>vary</td>
 * <td>Entries in order by key</td>
 * </tr>
 * <tr>
 * <td>restart index</td>
 * <td>vary</td>
 * <td>4 * restart count</td>
 * <td>Index of prefix compression restarts</td>
 * </tr>
 * <tr>
 * <td>restart count</td>
 * <td>0</td>
 * <td>4</td>
 * <td>Number of prefix compression restarts (used as index into entries)</td>
 * </tr>
 * </tbody>
 * </table>
 */
public class Block<T>
        extends ReferenceCounted<Block<T>>
        implements SeekingIterable<T, ByteBuffer>
{
    private final ByteBuffer block;
    private final Comparator<T> comparator;

    private final ByteBuffer data;
    private final ByteBuffer restartPositions;
    private final MemoryManager memory;
    private Decoder<T> decoder;
    private final Closeable cleaner;

    public Block(ByteBuffer block,
            Comparator<T> comparator,
            MemoryManager memory,
            Decoder<T> decoder,
            Closeable cleaner)
    {
        Preconditions.checkNotNull(block, "block is null");
        Preconditions.checkArgument(block.remaining() >= SIZE_OF_INT, "Block is corrupt: size must be at least %s block", SIZE_OF_INT);
        Preconditions.checkNotNull(comparator, "comparator is null");

        block = ByteBuffers.slice(block);
        this.block = block;
        this.comparator = comparator;
        this.memory = memory;
        this.decoder = decoder;
        this.cleaner = cleaner;

        // Keys are prefix compressed.  Every once in a while the prefix compression is restarted and the full key is written.
        // These "restart" locations are written at the end of the file, so you can seek to key without having to read the
        // entire file sequentially.

        // key restart count is the last int of the block
        int restartCount = block.getInt(block.limit() - SIZE_OF_INT);

        if (restartCount > 0) {
            // restarts are written at the end of the block
            int restartOffset = block.limit() - (1 + restartCount) * SIZE_OF_INT;
            Preconditions.checkArgument(restartOffset < block.limit() - SIZE_OF_INT, "Block is corrupt: restart offset count is greater than block size");
            restartPositions = ByteBuffers.duplicate(block, restartOffset, restartOffset + restartCount*SIZE_OF_INT);

            // data starts at 0 and extends to the restart index
            data = ByteBuffers.duplicate(block, block.position(), restartOffset);
        }
        else {
            data = ByteBuffers.EMPTY_BUFFER;
            restartPositions = ByteBuffers.EMPTY_BUFFER;
        }
    }

    public long size()
    {
        return block.remaining();
    }

    @Override
    public BlockIterator<T> iterator()
    {
        return new BlockIterator<T>(data, restartPositions, comparator, memory, decoder, this);
    }

    @Override
    protected Block<T> getThis()
    {
        return this;
    }

    @Override
    protected void dispose()
    {
        try {
            cleaner.close();
        }
        catch (Exception e) {
            Throwables.propagate(e);
        }
    }
}
