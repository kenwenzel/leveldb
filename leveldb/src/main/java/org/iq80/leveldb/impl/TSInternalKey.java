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

import org.iq80.leveldb.util.Slice;
import org.iq80.leveldb.util.SliceOutput;
import org.iq80.leveldb.util.Slices;

public class TSInternalKey extends InternalKey {
    public TSInternalKey(Slice userKey, long sequenceNumber, ValueType valueType) {
	super(userKey, sequenceNumber, valueType);
    }

    @Override
    public Slice encode() {
	Slice slice = Slices.allocate(Math.max(userKey.length(), 8) + 2);
	SliceOutput sliceOutput = slice.output();
	sliceOutput.writeBytes(userKey);
	sliceOutput.writeByte((int) sequenceNumber);
	sliceOutput.writeByte(valueType.getPersistentId());
	return slice;
    }
}
