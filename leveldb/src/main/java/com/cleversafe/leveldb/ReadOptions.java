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
package com.cleversafe.leveldb;

public class ReadOptions
        implements Cloneable
{
    private static final ReadOptions DEFAULT_READ_OPTIONS = OptionsUtil.populateFromProperties(
            "leveldb.readOptions.", new ReadOptions(null));

    /**
     * @deprecated use {@link ReadOptions#make()}
     */
    public ReadOptions()
    {
        this(DEFAULT_READ_OPTIONS);
    }

    private ReadOptions(ReadOptions that)
    {
        OptionsUtil.copyFields(ReadOptions.class, that, this);
    }

    public static ReadOptions make()
    {
        return copy(DEFAULT_READ_OPTIONS);
    }

    public static ReadOptions copy(ReadOptions other)
    {
        if (other == null)
            throw new IllegalArgumentException("copy target cannot be null");
        try {
            return (ReadOptions) other.clone();
        }
        catch (CloneNotSupportedException e) {
            return new ReadOptions(DEFAULT_READ_OPTIONS);
        }
    }

    @Override
    public String toString()
    {
        return OptionsUtil.toString(this);
    }

    private boolean verifyChecksums = false;
    private boolean fillCache = true;
    private Snapshot snapshot = null;

    public Snapshot snapshot()
    {
        return snapshot;
    }

    /**
     * If "snapshot" is non-NULL, read as of the supplied snapshot (which must
     * belong to the DB that is being read and which must not have been
     * released). If "snapshot" is NULL, use an implicit snapshot of the state
     * at the beginning of this read operation.
     */
    public ReadOptions snapshot(Snapshot snapshot)
    {
        this.snapshot = snapshot;
        return this;
    }

    public boolean fillCache()
    {
        return fillCache;
    }

    /**
     * Should the data read for this iteration be cached in memory? Callers may
     * wish to set this field to false for bulk scans.
     */
    public ReadOptions fillCache(boolean fillCache)
    {
        this.fillCache = fillCache;
        return this;
    }

    public boolean verifyChecksums()
    {
        return verifyChecksums;
    }

    /**
     * If true, all data read from underlying storage will be verified against
     * corresponding checksums.
     */
    public ReadOptions verifyChecksums(boolean verifyChecksums)
    {
        this.verifyChecksums = verifyChecksums;
        return this;
    }
}
