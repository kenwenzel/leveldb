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
package org.iq80.leveldb.util;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.iq80.leveldb.MemoryManager;

public class MemoryManagers
{
    private MemoryManagers()
    {
    }

    private enum Managers
            implements MemoryManager
    {
        DIRECT
        {

            @Override
            public ByteBuffer allocate(int capacity)
            {
                return ByteBuffer.allocateDirect(capacity).order(ByteOrder.LITTLE_ENDIAN);
            }

            @Override
            public void free(ByteBuffer buffer)
            {
                ByteBuffers.freeDirect(buffer);
            }
        },
        HEAP
        {
            @Override
            public ByteBuffer allocate(int size)
            {
                return ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
            }

            @Override
            public void free(ByteBuffer buffer)
            {
                // noop
            }

        }
    }

    /**
     * simple standard lib direction allocation. generally performs poorly for
     * naive allocations, not recommended for production use
     */
    public static MemoryManager direct()
    {
        return Managers.DIRECT;
    }

    public static MemoryManager heap()
    {
        return Managers.HEAP;
    }
}
