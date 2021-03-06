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

import java.util.Comparator;

import org.iq80.leveldb.impl.SeekingIterator;
import org.iq80.leveldb.table.Block;
import org.iq80.leveldb.util.Slice;

public class TSBlock extends Block {
    public static final int HEADER_LAST_VALUE = 1 << 8;
    public static final int HEADER_VALUE_LENGTH_ENCODED = 1 << 9;
    
    public TSBlock(Slice block, Comparator<Slice> comparator) {
	super(block, comparator);
    }

    @Override
    public SeekingIterator<Slice, Slice> iterator() {
	return new TSBlockIterator(data, restartPositions, comparator);
    }
}
