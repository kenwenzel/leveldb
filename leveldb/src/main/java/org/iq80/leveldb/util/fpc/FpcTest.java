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

import java.nio.ByteBuffer;

public class FpcTest {
	public static void main(String... args) {
		FpcCompressor comp = new FpcCompressor();

		double[] template = { 0.0, 0.0123, 0.0532324, 0.02, 0.03344 };
		double[] values = new double[31];
		for (int idx = 0; idx < values.length; idx++) {
			values[idx] = template[idx % template.length];
		}

		ByteBuffer bb = ByteBuffer.allocate(values.length * 8);
		comp.compress(bb, values);

		System.out.println(bb.position() / (double) values.length);
		

		bb.flip();
		double[] values2 = new double[values.length];
		comp.decompress(bb, values2);
	}
}
