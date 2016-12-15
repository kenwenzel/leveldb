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
package org.iq80.leveldb.impl;

import com.google.common.base.Preconditions;
import org.iq80.leveldb.table.UserComparator;
import org.iq80.leveldb.util.Slice;

public class InternalUserComparator
        implements UserComparator
{
    private final InternalKeyFactory internalKeyFactory;
    private final InternalKeyComparator internalKeyComparator;

    public InternalUserComparator(InternalKeyFactory internalKeyFactory, InternalKeyComparator internalKeyComparator)
    {
	this.internalKeyFactory = internalKeyFactory;
        this.internalKeyComparator = internalKeyComparator;
    }

    @Override
    public int compare(Slice left, Slice right)
    {
        return internalKeyComparator.compare(internalKeyFactory.createInternalKey(left), internalKeyFactory.createInternalKey(right));
    }

    @Override
    public String name()
    {
        return internalKeyComparator.name();
    }

    @Override
    public Slice findShortestSeparator(
            Slice start,
            Slice limit)
    {
        // Attempt to shorten the user portion of the key
        Slice startUserKey = internalKeyFactory.createInternalKey(start).getUserKey();
        Slice limitUserKey = internalKeyFactory.createInternalKey(limit).getUserKey();

        Slice shortestSeparator = internalKeyComparator.getUserComparator().findShortestSeparator(startUserKey, limitUserKey);

        if (internalKeyComparator.getUserComparator().compare(startUserKey, shortestSeparator) < 0) {
            // User key has become larger.  Tack on the earliest possible
            // number to the shortened user key.
            InternalKey newInternalKey = internalKeyFactory.createInternalKey(shortestSeparator, internalKeyFactory.maxSequenceNumber(), ValueType.VALUE);
            Preconditions.checkState(compare(start, newInternalKey.encode()) < 0); // todo
            Preconditions.checkState(compare(newInternalKey.encode(), limit) < 0); // todo

            return newInternalKey.encode();
        }

        return start;
    }

    @Override
    public Slice findShortSuccessor(Slice key)
    {
        Slice userKey = internalKeyFactory.createInternalKey(key).getUserKey();
        Slice shortSuccessor = internalKeyComparator.getUserComparator().findShortSuccessor(userKey);

        if (internalKeyComparator.getUserComparator().compare(userKey, shortSuccessor) < 0) {
            // User key has become larger.  Tack on the earliest possible
            // number to the shortened user key.
            InternalKey newInternalKey = internalKeyFactory.createInternalKey(shortSuccessor, internalKeyFactory.maxSequenceNumber(), ValueType.VALUE);
            Preconditions.checkState(compare(key, newInternalKey.encode()) < 0); // todo

            return newInternalKey.encode();
        }

        return key;
    }
}
