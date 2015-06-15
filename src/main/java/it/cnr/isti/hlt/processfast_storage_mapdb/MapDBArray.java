/*
 * *****************
 *  Copyright 2015 Tiziano Fagni (tiziano.fagni@isti.cnr.it)
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
 * *******************
 */

package it.cnr.isti.hlt.processfast_storage_mapdb;

import it.cnr.isti.hlt.processfast.data.*;
import it.cnr.isti.hlt.processfast.utils.Pair;
import org.mapdb.*;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentNavigableMap;

/**
 * Created by Tiziano on 10/06/2015.
 */
public class MapDBArray<T extends Serializable> implements Array<T> {

    private static final String ARRAY_PREFIX = "storage_array_";
    private static final int MAX_NUM_RETRIES = 10;
    private static final String NUM_ITEMS_STORED_PREFIX = "arr_num_items_stored_";
    private static final String DEFAULT_VALUE_PREFIX = "arr_default_value_";

    private final long arrayID;
    private MapDBStorage storage;
    private final String name;


    public MapDBArray(MapDBStorage storage, String name, long arrayID) {
        if (storage == null)
            throw new NullPointerException("The parent storage is 'null'");
        if (name == null || name.isEmpty())
            throw new IllegalArgumentException("The name is 'null' or empty");
        this.storage = storage;
        this.arrayID = arrayID;
        this.name = name;

        // Create array structure, if not available.
        DBUtils.atomic(storage.sm.provider.txMaker(), MAX_NUM_RETRIES, db -> {
            if (!db.exists(getInternalMapName())) {
                BTreeMap<Long, T> map = db.createTreeMap(getInternalMapName()).counterEnable().valueSerializer(Serializer.JAVA).makeOrGet();
                db.createAtomicLong(getInternalNumItemsStored(), 0);
            }
        });

    }


    protected String getInternalMapName() {
        return getInternalMapName(storage.getStorageID(), arrayID);
    }

    protected static String getInternalMapName(long storageID, long arrayID) {
        return ARRAY_PREFIX + storageID + "_" + arrayID;
    }

    protected String getInternalNumItemsStored() {
        return getInternalNumItemsStored(storage.getStorageID(), arrayID);
    }

    protected static String getInternalNumItemsStored(long storageID, long arrayID) {
        return NUM_ITEMS_STORED_PREFIX + storageID + "_" + arrayID;
    }

    protected String getDefaultValueName() {
        return getDefaultValueName(storage.getStorageID(), arrayID);
    }

    protected static String getDefaultValueName(long storageID, long arrayID) {
        return DEFAULT_VALUE_PREFIX + storageID + "_" + arrayID;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public long size() {
        DB tx = storage.sm.provider.tx();
        try {
            return size(tx);
        } finally {
            tx.close();
        }
    }

    protected long size(DB db) {
        Atomic.Long num = db.getAtomicLong(getInternalNumItemsStored());
        return num.get();
    }

    @Override
    public T getValue(long index) {
        long s = size();
        if (index < 0 || index >= s)
            throw new IllegalArgumentException("The index " + index + " is not valid. Valid range values: [" + 0 + "," + s + "]");


        return DBUtils.atomicGet(storage.sm.provider.txMaker(), MAX_NUM_RETRIES, db -> {
            BTreeMap<Long, T> array = db.getTreeMap(getInternalMapName());
            T v = array.get(index);
            if (v == null)
                return getDefaultValue(db);
            else
                return v;
        });

    }

    @Override
    public List<T> getValues(long fromIndex, long toIndex) {
        long curSize = size();
        long to = toIndex;
        if (fromIndex >= to)
            return new ArrayList<>();
        if (to >= curSize)
            to = curSize;
        if (fromIndex < 0)
            throw new IllegalArgumentException("The fromIndex value is less than 0");

        long toAdd = to - fromIndex;
        List<T> values = new ArrayList<T>((int) toAdd);

        return DBUtils.atomicGet(storage.sm.provider.txMaker(), MAX_NUM_RETRIES, db -> {
            BTreeMap<Long, T> array = db.getTreeMap(getInternalMapName());
            ArrayList<T> ret = new ArrayList<T>();
            T defaultValue = getDefaultValue(db);
            for (int i = 0; i < toAdd; i++) {
                T v = array.get(fromIndex + i);
                if (v == null)
                    ret.add(defaultValue);
                else
                    ret.add(v);
            }
            return ret;
        });

    }

    @Override
    public void setValue(long index, T value) {
        long s = size();
        if (index < 0 || index >= s)
            throw new IllegalArgumentException("The index " + index + " is not valid. Valid range values: [" + 0 + "," + s + "]");


        DBUtils.atomic(storage.sm.provider.txMaker(), MAX_NUM_RETRIES, db -> {
            setValue(db, index, value);
        });

    }


    protected void setValue(DB db, long index, T value) {
        BTreeMap<Long, T> array = db.getTreeMap(getInternalMapName());
        if (value != null)
            array.put(index, value);
        else
            array.remove(index);
    }


    @Override
    public void appendValue(T value) {
        if (value == null)
            throw new NullPointerException("The specified value is 'null'");

        DBUtils.atomic(storage.sm.provider.txMaker(), MAX_NUM_RETRIES, db -> {
            long s = size(db);
            resize(db, s + 1);
            setValue(db, s, value);
        });
    }

    @Override
    public void appendValues(long numItems, T value) {
        if (value == null)
            throw new NullPointerException("The specified value is 'null'");

        DBUtils.atomic(storage.sm.provider.txMaker(), MAX_NUM_RETRIES, db -> {
            long s = size(db);
            resize(db, s + numItems);
            for (long i = s; i < s + numItems; i++)
                setValue(db, i, value);
        });

    }

    @Override
    public T getDefaultValue() {
        return DBUtils.atomicGet(storage.sm.provider.txMaker(), MAX_NUM_RETRIES, db -> {
            return getDefaultValue(db);
        });

    }


    protected T getDefaultValue(DB db) {
        Atomic.Var<T> val = db.getAtomicVar(getDefaultValueName());
        return val.get();
    }

    @Override
    public void setDefaultValue(T defaultValue) {
        DBUtils.atomic(storage.sm.provider.txMaker(), MAX_NUM_RETRIES, db -> {
            Atomic.Var<T> val = db.getAtomicVar(getDefaultValueName());
            val.set(defaultValue);
        });

    }

    @Override
    public void clear() {
        DBUtils.atomic(storage.sm.provider.txMaker(), MAX_NUM_RETRIES, db -> {
            clear(db);
        });

    }

    protected void clear(DB db) {
        resize(db, 0);
    }

    @Override
    public void resize(long newSize) {
        if (newSize < 0)
            throw new IllegalArgumentException("The new size is less than 0");
        DBUtils.atomic(storage.sm.provider.txMaker(), MAX_NUM_RETRIES, db -> {
            resize(db, newSize);
        });
    }

    protected void resize(DB db, long newSize) {
        long curSize = size(db);
        if (newSize < curSize) {
            // Remove all items outside new size.
            BTreeMap<Long, T> array = db.getTreeMap(getInternalMapName());
            ConcurrentNavigableMap<Long, T> subMap = array.subMap(newSize, true, curSize, true);
            Iterator<Long> keys = subMap.keySet().iterator();
            ArrayList<Long> keysToRemove = new ArrayList<>();
            while (keys.hasNext())
                keysToRemove.add(keys.next());
            for (long k : keysToRemove)
                array.remove(k);
        }
        Atomic.Long numItemsStored = db.getAtomicLong(getInternalNumItemsStored());
        numItemsStored.set(newSize);
    }

    @Override
    public Iterator<T> asIterator(long numBufferedItems) {
        return new ArrayIterator<T>(this, numBufferedItems);
    }

    @Override
    public Array<T> copyFrom(Array<T> source, boolean clearArrayContent, long numBufferedItems) {
        if (source == null)
            throw new NullPointerException("The source array is 'null'");

        return DBUtils.atomicGet(storage.sm.provider.txMaker(), MAX_NUM_RETRIES, db -> {
            return copyFrom(db, source, clearArrayContent, numBufferedItems);
        });

    }

    protected Array<T> copyFrom(DB db, Array<T> source, boolean clearArrayContent, long numBufferedItems) {
        if (source == null)
            throw new NullPointerException("The source array is 'null'");

        if (clearArrayContent)
            clear(db);
        long sourceSize = source.size();
        long startFrom = size(db);
        resize(db, startFrom + sourceSize);
        long numRead = 0;
        boolean done = false;
        while (!done) {
            List<T> values = source.getValues(numRead, numRead + numBufferedItems);
            if (values.size() == 0) {
                done = true;
                break;
            }

            for (int i = 0; i < values.size(); i++) {
                setValue(db, startFrom + numRead + i, values.get(i));
            }

            numRead += values.size();
        }
        return this;
    }

    @Override
    public Array<T> copyFrom(Collection<T> source, boolean clearArrayContent, long numBufferedItems) {
        if (source == null)
            throw new NullPointerException("The source array is 'null'");

        return DBUtils.atomicGet(storage.sm.provider.txMaker(), MAX_NUM_RETRIES, db -> {
            return copyFrom(db, source, clearArrayContent, numBufferedItems);
        });
    }

    Array<T> copyFrom(DB db, Collection<T> source, boolean clearArrayContent, long numBufferedItems) {
        if (source == null)
            throw new NullPointerException("The source collection is 'null'");

        Iterator<T> iter = source.iterator();

        if (clearArrayContent)
            clear(db);
        long sourceSize = source.size();
        long startFrom = size(db);
        resize(db, startFrom + sourceSize);
        long numRead = 0;
        boolean done = false;
        while (!done) {
            List<T> values = readValuesFromIterator(iter, numBufferedItems);
            if (values.size() == 0) {
                done = true;
                break;
            }

            for (int i = 0; i < values.size(); i++) {
                setValue(db, startFrom + numRead + i, values.get(i));
            }
            numRead += values.size();
        }
        return this;
    }

    protected List<T> readValuesFromIterator(java.util.Iterator<T> iter, long numToRead) {
        List<T> l = new ArrayList<>();
        while (iter.hasNext()) {
            l.add(iter.next());
        }
        return l;
    }

    @Override
    public void copyTo(Collection<T> dest, boolean clearList, long numBufferedItems) {
        if (clearList)
            dest.clear();
        ArrayIterator<T> iter = new ArrayIterator<T>(this, numBufferedItems);
        while (iter.hasNext()) {
            T item = iter.next();
            dest.add(item);
        }
    }

    @Override
    public void copyTo(Array<T> dest, boolean clearArray, long numBufferedItems) {
        dest.copyFrom(this, clearArray, numBufferedItems);
    }

    @Override
    public ImmutableDataSourceIteratorProvider<T> asIteratorProvider(long numBufferedItems) {
        if (numBufferedItems < 1)
            throw new IllegalArgumentException("The number of buffered items is less than 1");
        return new ArrayDataSourceIteratorProvider<T>(this, numBufferedItems);
    }

    @Override
    public ImmutableDataSourceIteratorProvider<Pair<Long, T>> asIteratorProviderWithIndex(long numBufferedItems) {
        if (numBufferedItems < 1)
            throw new IllegalArgumentException("The number of buffered items is less than 1");
        return new ArrayPairDataSourceIteratorProvider<T>(this, numBufferedItems);
    }

    @Override
    public void enableLocalCache(boolean enabled, long fromIndex, long toIndex) {

    }

    @Override
    public boolean isLocalCacheEnabled(long index) {
        return false;
    }

    @Override
    public void flush() {

    }

    public static void removeArray(DB db, long storageID, long arrayID) {
        db.delete(getInternalMapName(storageID, arrayID));
        db.delete(getInternalNumItemsStored(storageID, arrayID));
        db.delete(getDefaultValueName(storageID, arrayID));
    }
}
