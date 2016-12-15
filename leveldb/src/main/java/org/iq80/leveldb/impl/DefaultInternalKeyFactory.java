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

public class DefaultInternalKeyFactory implements InternalKeyFactory {
    // We leave eight bits empty at the bottom so a type and sequence#
    // can be packed together into 64-bits.
    public static final long MAX_SEQUENCE_NUMBER = ((0x1L << 56) - 1);

    @Override
    public InternalKey createInternalKey(Slice data) {
	Preconditions.checkNotNull(data, "data is null");
	Preconditions.checkArgument(data.length() >= SIZE_OF_LONG, "data must be at least %s bytes", SIZE_OF_LONG);
	Slice userKey = getUserKey(data);
	long packedSequenceAndType = data.getLong(data.length() - SIZE_OF_LONG);
	long sequenceNumber = unpackSequenceNumber(packedSequenceAndType);
	ValueType valueType = unpackValueType(packedSequenceAndType);

	return new InternalKey(userKey, sequenceNumber, valueType);
    }

    @Override
    public InternalKey createInternalKey(Slice userKey, long sequenceNumber, ValueType valueType) {
	return new InternalKey(userKey, sequenceNumber, valueType);
    }

    private static Slice getUserKey(Slice data) {
	return data.slice(0, data.length() - SIZE_OF_LONG);
    }

    @Override
    public long maxSequenceNumber() {
	return MAX_SEQUENCE_NUMBER;
    }

    public static long packSequenceAndValueType(long sequence, ValueType valueType) {
	Preconditions.checkArgument(sequence <= MAX_SEQUENCE_NUMBER,
		"Sequence number is greater than MAX_SEQUENCE_NUMBER");
	Preconditions.checkNotNull(valueType, "valueType is null");

	return (sequence << 8) | valueType.getPersistentId();
    }

    public static ValueType unpackValueType(long num) {
	return ValueType.getValueTypeByPersistentId((byte) num);
    }

    public static long unpackSequenceNumber(long num) {
	return num >>> 8;
    }
}
