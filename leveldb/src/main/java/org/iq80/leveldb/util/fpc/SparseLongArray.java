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

/**
 * SparseArrays map integers to Objects. Unlike a normal array of Objects, there
 * can be gaps in the indices. It is intended to be more memory efficient than
 * using a HashMap to map Integers to Objects, both because it avoids
 * auto-boxing keys and its data structure doesn't rely on an extra entry object
 * for each mapping.
 *
 * <p>
 * Note that this container keeps its mappings in an array data structure, using
 * a binary search to find keys. The implementation is not intended to be
 * appropriate for data structures that may contain large numbers of items. It
 * is generally slower than a traditional HashMap, since lookups require a
 * binary search and adds and removes require inserting and deleting entries in
 * the array. For containers holding up to hundreds of items, the performance
 * difference is not significant, less than 50%.
 * </p>
 *
 * <p>
 * Removal of keys is not supported by this implementation.
 * </p>
 *
 * <p>
 * It is possible to iterate over the items in this container using
 * {@link #keyAt(int)} and {@link #valueAt(int)}. Iterating over the keys using
 * <code>keyAt(int)</code> with ascending values of the index will return the
 * keys in ascending order, or the values corresponding to the keys in ascending
 * order in the case of <code>valueAt(int)</code>.
 * </p>
 */
public class SparseLongArray implements Cloneable {
    private int[] mKeys;
    private long[] mValues;
    private int mSize;

    static final int[] EMPTY_INTS = new int[0];
    static final long[] EMPTY_VALUES = new long[0];

    /**
     * Creates a new SparseArray containing no mappings.
     */
    public SparseLongArray() {
	this(10);
    }

    /**
     * Creates a new SparseArray containing no mappings that will not require
     * any additional memory allocation to store the specified number of
     * mappings. If you supply an initial capacity of 0, the sparse array will
     * be initialized with a light-weight representation not requiring any
     * additional array allocations.
     */
    public SparseLongArray(int initialCapacity) {
	if (initialCapacity == 0) {
	    mKeys = EMPTY_INTS;
	    mValues = EMPTY_VALUES;
	} else {
	    initialCapacity = idealIntArraySize(initialCapacity);
	    mKeys = new int[initialCapacity];
	    mValues = new long[initialCapacity];
	}
	mSize = 0;
    }

    @Override
    public SparseLongArray clone() {
	SparseLongArray clone = null;
	try {
	    clone = (SparseLongArray) super.clone();
	    clone.mKeys = mKeys.clone();
	    clone.mValues = mValues.clone();
	} catch (CloneNotSupportedException cnse) {
	    /* ignore */
	}
	return clone;
    }

    /**
     * Gets the Object mapped from the specified key, or <code>null</code> if no
     * such mapping has been made.
     */
    public long get(int key) {
	return get(key, 0);
    }

    /**
     * Gets the value mapped from the specified key, or the specified value if
     * no such mapping has been made.
     */
    public long get(int key, long valueIfKeyNotFound) {
	int i = binarySearch(mKeys, mSize, key);

	if (i < 0) {
	    return valueIfKeyNotFound;
	} else {
	    return mValues[i];
	}
    }

    /**
     * Adds a mapping from the specified key to the specified value, replacing
     * the previous mapping from the specified key if there was one.
     */
    public void put(int key, long value) {
	int i = binarySearch(mKeys, mSize, key);

	if (i >= 0) {
	    mValues[i] = value;
	} else {
	    i = ~i;

	    if (mSize >= mKeys.length) {
		int n = idealIntArraySize(mSize + 1);

		int[] nkeys = new int[n];
		long[] nvalues = new long[n];

		// Log.e("SparseArray", "grow " + mKeys.length + " to " + n);
		System.arraycopy(mKeys, 0, nkeys, 0, mKeys.length);
		System.arraycopy(mValues, 0, nvalues, 0, mValues.length);

		mKeys = nkeys;
		mValues = nvalues;
	    }

	    if (mSize - i != 0) {
		// Log.e("SparseArray", "move " + (mSize - i));
		System.arraycopy(mKeys, i, mKeys, i + 1, mSize - i);
		System.arraycopy(mValues, i, mValues, i + 1, mSize - i);
	    }

	    mKeys[i] = key;
	    mValues[i] = value;
	    mSize++;
	}
    }

    /**
     * Returns the number of key-value mappings that this SparseArray currently
     * stores.
     */
    public int size() {
	return mSize;
    }

    /**
     * Removes all key-value mappings from this SparseArray.
     */
    public void clear() {
	int n = mSize;
	long[] values = mValues;

	for (int i = 0; i < n; i++) {
	    values[i] = 0;
	}

	mSize = 0;
    }

    public static int idealByteArraySize(int need) {
	for (int i = 4; i < 32; i++)
	    if (need <= (1 << i) - 12)
		return (1 << i) - 12;

	return need;
    }

    public static int idealIntArraySize(int need) {
	return idealByteArraySize(need * 4) / 4;
    }

    // This is Arrays.binarySearch(), but doesn't do any argument validation.
    static int binarySearch(int[] array, int size, int value) {
	int lo = 0;
	int hi = size - 1;

	while (lo <= hi) {
	    final int mid = (lo + hi) >>> 1;
	    final int midVal = array[mid];

	    if (midVal < value) {
		lo = mid + 1;
	    } else if (midVal > value) {
		hi = mid - 1;
	    } else {
		return mid; // value found
	    }
	}
	return ~lo; // value not present
    }

    static int binarySearch(long[] array, int size, long value) {
	int lo = 0;
	int hi = size - 1;

	while (lo <= hi) {
	    final int mid = (lo + hi) >>> 1;
	    final long midVal = array[mid];

	    if (midVal < value) {
		lo = mid + 1;
	    } else if (midVal > value) {
		hi = mid - 1;
	    } else {
		return mid; // value found
	    }
	}
	return ~lo; // value not present
    }
}