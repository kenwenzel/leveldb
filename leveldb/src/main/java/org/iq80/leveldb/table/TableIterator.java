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

import org.iq80.leveldb.impl.InternalKey;
import org.iq80.leveldb.util.AbstractReverseSeekingIterator;
import org.iq80.leveldb.util.InternalIterator;

import java.nio.ByteBuffer;
import java.util.Map.Entry;

import static org.iq80.leveldb.table.TableIterator.CurrentOrigin.*;

public final class TableIterator
        extends AbstractReverseSeekingIterator<InternalKey, ByteBuffer>
        implements InternalIterator
{
    private final Table table;
    private final BlockIterator<InternalKey> indexIterator;
    private BlockIterator<InternalKey> current;
    private CurrentOrigin currentOrigin = NONE;
    private boolean closed;

    public enum CurrentOrigin
    {
        /*
         * reversable iterators don't have a consistent concept of a "current" item instead they exist
         * in a position "between" next and prev. in order to make the BlockIterator 'current' work,
         * we need to track from which direction it was initialized so that calls to advance the
         * encompassing 'blockIterator' are consistent
         */
        PREV, NEXT, NONE
        // a state of NONE should be interchangeable with current==NULL
    }

    public TableIterator(Table table, BlockIterator<InternalKey> indexIterator)
    {
        this.table = table;
        this.closed = false;
        this.indexIterator = indexIterator;
        clearCurrent();
    }

    @Override
    public void close()
    {
        if (!closed) {
            closed = true;
            closeCurrent();
            indexIterator.close();
            table.close();
        }
    }

    @Override
    protected void seekToFirstInternal()
    {
        // reset index to before first and clear the data iterator
        indexIterator.seekToFirst();
        clearCurrent();
    }

    @Override
    protected void seekToLastInternal()
    {
        indexIterator.seekToEnd();
        clearCurrent();
        if (currentHasPrev()) {
            current.prev();
        }
    }

    @Override
    public void seekToEndInternal()
    {
        indexIterator.seekToEnd();
        clearCurrent();
    }

    @Override
    protected void seekInternal(InternalKey targetKey)
    {
        // seek the index to the block containing the key
        indexIterator.seek(targetKey);

        // if indexIterator does not have a next, it mean the key does not exist in this iterator
        if (indexIterator.hasNext()) {
            // seek the current iterator to the key
            current = getNextBlock();
            current.seek(targetKey);
        }
        else {
            clearCurrent();
        }
    }

    @Override
    protected boolean hasNextInternal()
    {
        return currentHasNext();
    }

    @Override
    protected boolean hasPrevInternal()
    {
        return currentHasPrev();
    }

    @Override
    protected Entry<InternalKey, ByteBuffer> getNextElement()
    {
        // note: it must be here & not where 'current' is assigned,
        // because otherwise we'll have called inputs.next() before throwing
        // the first NPE, and the next time around we'll call inputs.next()
        // again, incorrectly moving beyond the error.
        return currentHasNext() ? current.next() : null;
    }

    @Override
    protected Entry<InternalKey, ByteBuffer> getPrevElement()
    {
        return currentHasPrev() ? current.prev() : null;
    }

    @Override
    protected Entry<InternalKey, ByteBuffer> peekInternal()
    {
        return currentHasNext() ? current.peek() : null;
    }

    @Override
    protected Entry<InternalKey, ByteBuffer> peekPrevInternal()
    {
        return currentHasPrev() ? current.peekPrev() : null;
    }

    private boolean currentHasNext()
    {
        boolean currentHasNext = false;
        while (true) {
            if (current != null) {
                currentHasNext = current.hasNext();
            }
            if (!currentHasNext) {
                if (currentOrigin == PREV) {
                    // current came from PREV, so advancing indexIterator to next() must be safe
                    // indeed, because alternating calls to prev() and next() must return the same item
                    // current can be retrieved from next() when the origin is PREV
                    indexIterator.next();
                    // but of course we want to go beyond current to the next block
                    // so we pass into the next if
                }
                if (indexIterator.hasNext()) {
                    current = getNextBlock();
                }
                else {
                    break;
                }
            }
            else {
                break;
            }
        }
        if (!currentHasNext) {
            clearCurrent();
        }
        return currentHasNext;
    }

    private boolean currentHasPrev()
    {
        boolean currentHasPrev = false;
        while (true) {
            if (current != null) {
                currentHasPrev = current.hasPrev();
            }
            if (!(currentHasPrev)) {
                if (currentOrigin == NEXT) {
                    indexIterator.prev();
                }
                if (indexIterator.hasPrev()) {
                    current = getPrevBlock();
                    current.seekToEnd();
                }
                else {
                    break;
                }
            }
            else {
                break;
            }
        }
        if (!currentHasPrev) {
            clearCurrent();
        }
        return currentHasPrev;
    }

    private BlockIterator<InternalKey> getNextBlock()
    {
        ByteBuffer blockHandle = indexIterator.next().getValue();
        currentOrigin = NEXT;
        return blockIterator(blockHandle);
    }

    private BlockIterator<InternalKey> getPrevBlock()
    {
        ByteBuffer blockHandle = indexIterator.prev().getValue();
        currentOrigin = PREV;
        return blockIterator(blockHandle);
    }

    private BlockIterator<InternalKey> blockIterator(ByteBuffer blockHandle)
    {
        closeCurrent();
        try (Block<InternalKey> dataBlock = table.openBlock(blockHandle)) {
            return dataBlock.iterator(); // dataBlock retained by iterator
        }
    }

    private void closeCurrent()
    {
        if (current != null) {
            current.close();
        }
    }

    private void clearCurrent()
    {
        closeCurrent();
        current = null;
        currentOrigin = NONE;
    }

    @Override
    public String toString()
    {
        final StringBuilder sb = new StringBuilder();
        sb.append("ConcatenatingIterator");
        sb.append("{blockIterator=").append(indexIterator);
        sb.append(", current=").append(current);
        sb.append('}');
        return sb.toString();
    }
}
