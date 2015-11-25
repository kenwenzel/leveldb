/*
 * Copyright (C) 2011 the original author or authors. See the notice.md file distributed with this
 * work for additional information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.cleversafe.leveldb.impl;

import java.nio.ByteBuffer;
import java.util.Objects;

import com.cleversafe.leveldb.util.ByteBuffers;
import com.cleversafe.leveldb.util.GrowingBuffer;
import com.cleversafe.leveldb.util.SizeOf;

public abstract class InternalKey {
  public static final InternalKey MINIMUM_KEY = new TransientInternalKey(ByteBuffers.EMPTY_BUFFER,
      0, ValueType.getValueTypeByPersistentId((byte) 0));

  protected final ByteBuffer userKey;

  protected InternalKey(final ByteBuffer userKey) {
    this.userKey = userKey;
  }

  public final ByteBuffer getUserKey() {
    return userKey;
  }

  public abstract long getSequenceNumber();

  public abstract ValueType getValueType();

  public abstract ByteBuffer writeToBuffer(ByteBuffer dst);

  public abstract ByteBuffer writeUnsharedAndValue(GrowingBuffer buffer, boolean restart,
      ByteBuffer lastKeyBuffer, ByteBuffer value);

  public final int getEncodedSize() {
    return getUserKey().remaining() + SizeOf.SIZE_OF_LONG;
  }

  public final InternalKey heapCopy() {
    return new TransientInternalKey(ByteBuffers.heapCopy(getUserKey()), getSequenceNumber(),
        getValueType());
  }

  @Override
  public boolean equals(final Object o) {
    if (o == null) {
      return false;
    }
    if (!(o instanceof InternalKey)) {
      return false;
    }

    final InternalKey that = (InternalKey) o;
    return Objects.equals(this.getUserKey(), that.getUserKey())
        && this.getSequenceNumber() == that.getSequenceNumber()
        && Objects.equals(this.getValueType(), that.getValueType());
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + " [key=" + ByteBuffers.toString(getUserKey())
        + ", sequence=" + getSequenceNumber() + ", type=" + getValueType() + "]";
  }
}