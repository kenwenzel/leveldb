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

public class DfcmPredictor {

    private long[] table;
    private int dfcm_hash;
    private long lastValue;

    public DfcmPredictor(int logOfTableSize) {
        table = new long[1 << logOfTableSize];
    }

    public long getPrediction() {
        return table[dfcm_hash] + lastValue;
    }

    public void update(long true_value) {
        table[dfcm_hash] = true_value - lastValue;
        dfcm_hash = (int) (((dfcm_hash << 2) ^ ((true_value - lastValue) >> 40)) &
                (table.length - 1));
        lastValue = true_value;
    }
}
