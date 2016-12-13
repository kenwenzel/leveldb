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
        Preconditions.checkArgument(data.length() >= SIZE_OF_LONG, "data must be at least %s bytes", SIZE_OF_LONG);
        
        Slice userKey = data.slice(0, data.length() - 1);
        byte valueTypeNum = data.getByte(data.length() - 1);
        ValueType valueType = ValueType.getValueTypeByPersistentId((byte) valueTypeNum);
        long sequenceNumber = userKey.getLongBigEndian(userKey.length() - SIZE_OF_LONG);
        return new TSInternalKey(userKey, sequenceNumber, valueType);
    }
    
    @Override
    public InternalKey createInternalKey(Slice userKey, long sequenceNumber, ValueType valueType) {
	// TODO this is not correct at the moment
	userKey = userKey.copySlice();
	if (sequenceNumber == SequenceNumber.MAX_SEQUENCE_NUMBER) {
	    sequenceNumber = userKey.getLongBigEndian(userKey.length() - SIZE_OF_LONG);
	    sequenceNumber++;
	}
	userKey.setLongBigEndian(userKey.length() - SIZE_OF_LONG, sequenceNumber);
	return new TSInternalKey(userKey, sequenceNumber, valueType);
    }
}
