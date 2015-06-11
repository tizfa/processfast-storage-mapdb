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

import it.cnr.isti.hlt.processfast.data.Array;
import it.cnr.isti.hlt.processfast.data.ImmutableDataSourceIteratorProvider;
import it.cnr.isti.hlt.processfast.utils.Pair;
import org.mapdb.DB;

import java.io.Serializable;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * Created by Tiziano on 10/06/2015.
 */
public class MapDBArray<T extends Serializable> implements Array<T>{

    private long arrayID;
    private MapDBStorage storage;


    public MapDBArray(MapDBStorage storage, long arrayID) {
        if (storage == null)
            throw new NullPointerException("The parent storage is 'null'");
        this.storage = storage;
        this.arrayID = arrayID;
    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    public long size() {
        return 0;
    }

    @Override
    public T getValue(long index) {
        return null;
    }

    @Override
    public List<T> getValues(long fromIndex, long toIndex) {
        return null;
    }

    @Override
    public void setValue(long index, T value) {

    }

    @Override
    public void appendValue(T value) {

    }

    @Override
    public void appendValues(long numItems, T value) {

    }

    @Override
    public T getDefaultValue() {
        return null;
    }

    @Override
    public void setDefaultValue(T defaultValue) {

    }

    @Override
    public void clear() {

    }

    @Override
    public void resize(long newSize) {

    }

    @Override
    public Iterator<T> asIterator(long numBufferedItems) {
        return null;
    }

    @Override
    public Array<T> copyFrom(Array<T> source, boolean clearArrayContent, long numBufferedItems) {
        return null;
    }

    @Override
    public Array<T> copyFrom(Collection<T> source, boolean clearArrayContent, long numBufferedItems) {
        return null;
    }

    @Override
    public void copyTo(Collection<T> dest, boolean clearList, long numBufferedItems) {

    }

    @Override
    public void copyTo(Array<T> dest, boolean clearArray, long numBufferedItems) {

    }

    @Override
    public ImmutableDataSourceIteratorProvider<T> asIteratorProvider(long numBufferedItems) {
        return null;
    }

    @Override
    public ImmutableDataSourceIteratorProvider<Pair<Long, T>> asIteratorProviderWithIndex(long numBufferedItems) {
        return null;
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

    public static void removeArray(DB db, long arrayID) {

    }
}
