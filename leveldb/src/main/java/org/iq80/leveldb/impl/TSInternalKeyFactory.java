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

import static org.iq80.leveldb.util.SizeOf.SIZE_OF_LONG;

import org.iq80.leveldb.util.Slice;

import com.google.common.base.Preconditions;

public class TSInternalKeyFactory implements InternalKeyFactory {
    @Override
    public InternalKey createInternalKey(Slice data) {
	Preconditions.checkNotNull(data, "data is null");
	Preconditions.checkArgument(data.length() >= 1, "data must be at least %s bytes", 2);

	Slice userKey = data.slice(0, data.length() - 2);
	byte valueTypeNum = data.getByte(data.length() - 1);
	ValueType valueType = ValueType.getValueTypeByPersistentId((byte) valueTypeNum);
	long nr = getLongBigEndian(userKey, Math.max(userKey.length() - SIZE_OF_LONG, 0));
	long sequenceNumber = ((nr << 8) | 0xFF & data.getByte(data.length() - 2)) & Long.MAX_VALUE;
	return new TSInternalKey(userKey, sequenceNumber, valueType);
    }

    @Override
    public InternalKey createInternalKey(Slice userKey, long sequenceNumber, ValueType valueType) {
	// TODO find a working way to handle multiple sequence numbers
	sequenceNumber = 0xFF;
	
	long nr = getLongBigEndian(userKey, Math.max(userKey.length() - SIZE_OF_LONG, 0));
	long newSequenceNumber = ((nr << 8) | 0xFF & sequenceNumber) & Long.MAX_VALUE;
	return new TSInternalKey(userKey, newSequenceNumber, valueType);
    }
    
    public static long calcSequenceNumber(Slice userKey, long sequenceNumber) {
	long nr = getLongBigEndian(userKey, Math.max(userKey.length() - SIZE_OF_LONG, 0));
	sequenceNumber = ((nr << 8) | 0xFF & sequenceNumber) & Long.MAX_VALUE;
	return sequenceNumber;
    }

    protected static long getLongBigEndian(Slice slice, int index) {
	index += slice.getRawOffset();
	byte[] data = slice.getRawArray();
	long result = 0;
	int shift = 56;
	while (shift >= 0) {
	    if (index < data.length) {
		result |= ((long) data[index] & 0xff) << shift;
	    } else {
		result |= ((long) 0xff) << shift;
	    }
	    index += 1;
	    shift -= 8;
	}
	return result;
    }

    @Override
    public long maxSequenceNumber() {
	return Long.MAX_VALUE;
    }
}
