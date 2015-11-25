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
package com.cleversafe.leveldb.table;

import com.google.common.base.Preconditions;

import java.nio.ByteBuffer;
import java.util.Map.Entry;

/**
 * Binary Structure
 * <table summary="record format">
 * <tbody>
 * <thead>
 * <tr>
 * <th>name</th>
 * <th>offset</th>
 * <th>length</th>
 * <th>description</th>
 * </tr>
 * </thead>
 * <p/>
 * <tr>
 * <td>shared key length</td>
 * <td>0</td>
 * <td>vary</td>
 * <td>variable length encoded int: size of shared key prefix with the key from the previous entry</td>
 * </tr>
 * <tr>
 * <td>non-shared key length</td>
 * <td>vary</td>
 * <td>vary</td>
 * <td>variable length encoded int: size of non-shared key suffix in this entry</td>
 * </tr>
 * <tr>
 * <td>value length</td>
 * <td>vary</td>
 * <td>vary</td>
 * <td>variable length encoded int: size of value in this entry</td>
 * </tr>
 * <tr>
 * <td>non-shared key</td>
 * <td>vary</td>
 * <td>non-shared key length</td>
 * <td>non-shared key data</td>
 * </tr>
 * <tr>
 * <td>value</td>
 * <td>vary</td>
 * <td>value length</td>
 * <td>value data</td>
 * </tr>
 * </tbody>
 * </table>
 */
public class BlockEntry<T>
        implements Entry<T, ByteBuffer>
{
    private final T key;
    private final ByteBuffer value;
    private final ByteBuffer encodedKey;

    private BlockEntry(T key, ByteBuffer encodedKey, ByteBuffer value)
    {
        Preconditions.checkNotNull(key, "key is null");
        Preconditions.checkNotNull(value, "value is null");
        this.key = key;
        this.encodedKey = encodedKey;
        this.value = value;
    }

    public static <T> BlockEntry<T> of(T key, ByteBuffer encodedKey, ByteBuffer value)
    {
        return new BlockEntry<T>(key, encodedKey, value);
    }

    public static BlockEntry<ByteBuffer> of(ByteBuffer key, ByteBuffer value)
    {
        return new BlockEntry<ByteBuffer>(key, key, value);
    }

    @Override
    public T getKey()
    {
        return key;
    }

    @Override
    public ByteBuffer getValue()
    {
        return value;
    }

    public ByteBuffer getEncodedKey()
    {
        return encodedKey;
    }

    /**
     * @throws UnsupportedOperationException always
     */
    @Override
    public final ByteBuffer setValue(ByteBuffer value)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        BlockEntry<?> entry = (BlockEntry<?>) o;

        if (!key.equals(entry.key)) {
            return false;
        }
        if (!value.equals(entry.value)) {
            return false;
        }
        if (!encodedKey.equals(entry.encodedKey)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = key.hashCode();
        result = 31 * result + value.hashCode();
        return result;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("BlockEntry");
        sb.append("{key=").append(key.toString());
        sb.append(", value=").append(value.toString());
        sb.append('}');
        return sb.toString();
    }
}