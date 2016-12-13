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
package org.iq80.leveldb.util.fpc;

import java.util.Arrays;

public class FcmPredictor {

	private long[] table;
	private int fcm_hash;

	public FcmPredictor(int logOfTableSize) {
		table = new long[1 << logOfTableSize];
	}

	public long getPrediction() {
		return table[fcm_hash];
	}

	public void update(long true_value) {
		table[fcm_hash] = true_value;
		fcm_hash = (int) (((fcm_hash << 6) ^ (true_value >> 48)) & (table.length - 1));
	}
	
	public void reset() {
	    fcm_hash = 0;
	    Arrays.fill(table, 0);
	}
}
