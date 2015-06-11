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
import org.mapdb.DB;
import org.mapdb.HTreeMap;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by Tiziano on 09/06/2015.
 */
public class MapDBStorage implements Storage {

    private static final String ARRAY_PREFIX = "storage_array_";
    private static final String ARRAY_NUM_KEYS = "storage_array_keys";
    private static final String ARRAY_KEY_PREFIX = "storage_array_k_";
    private static final int MAX_NUM_RETRIES = 10;

    MapDBStorageManager sm;
    private long storageID;
    private String storageName;

    public MapDBStorage(MapDBStorageManager sm, String storageName, long storageID) {
        if (sm == null)
            throw new NullPointerException("The storage manager is 'null'");
        if (storageName == null || storageName.isEmpty())
            throw new IllegalArgumentException("The storage name is 'null' or empty");
        this.sm = sm;
        this.storageID = storageID;
        this.storageName = storageName;
    }

    @Override
    public String getName() {
        return storageName;
    }

    @Override
    public List<String> getArrayNames() {
        DB tx = sm.provider.tx();
        try {
            ArrayList<String> toRet = DBUtils.atomicGet(tx, MAX_NUM_RETRIES, db -> {
                HTreeMap<String, Long> mapStorages = db.getHashMap(ARRAY_PREFIX +storageID);
                Iterator<String> keys = mapStorages.keySet().iterator();
                ArrayList<String> ret = new ArrayList<>();
                while (keys.hasNext()) {
                    String key = keys.next();
                    if (key.equals(ARRAY_NUM_KEYS))
                        continue;
                    ret.add(key);
                }
                return ret;
            });
            return toRet;
        } finally {
            tx.close();
        }
    }

    @Override
    public boolean containsArrayName(String name) {
        if (name == null || name.isEmpty())
            throw new IllegalArgumentException("The name is 'null' or empty");
        DB tx = sm.provider.tx();
        try {
            return DBUtils.atomicGet(tx, MAX_NUM_RETRIES, db -> {
                HTreeMap<String, Long> mapStorages = db.getHashMap(ARRAY_PREFIX+storageID);
                return mapStorages.containsKey(computeArrayName(name));
            });
        } finally {
            tx.close();
        }
    }

    private String computeArrayName(String name) {
        return ARRAY_KEY_PREFIX + name;
    }

    @Override
    public <T extends Serializable> Array<T> createArray(String name, Class<T> cl) {
        if (name == null || name.isEmpty())
            throw new IllegalArgumentException("The name is 'null' or empty");

        long arrayIdx;
        DB tx = sm.provider.tx();
        try {
            arrayIdx = DBUtils.atomicGet(tx, MAX_NUM_RETRIES, db -> {
                HTreeMap<String, Long> mapArrays = db.getHashMap(ARRAY_PREFIX+storageID);
                long idx = 0;
                if (containsArrayName(name)) {
                    idx = mapArrays.get(computeArrayName(name));
                } else {
                    long nextID = 0;
                    mapArrays.putIfAbsent(ARRAY_NUM_KEYS, 0l);
                    nextID = mapArrays.get(ARRAY_NUM_KEYS);
                    mapArrays.put(computeArrayName(name), nextID);
                    idx = nextID;
                }
                return idx;
            });
        } finally {
            tx.close();
        }

        return new MapDBArray<T>(this, arrayIdx);
    }

    @Override
    public void removeArray(String name) {
        if (!containsArrayName(computeArrayName(name)))
            return;

        DB tx = sm.provider.tx();
        try {
            DBUtils.atomic(tx, MAX_NUM_RETRIES, db -> {
                HTreeMap<String, Long> mapArrays = db.getHashMap(ARRAY_PREFIX+storageID);
                long arrayID = mapArrays.get(computeArrayName(name));
                mapArrays.remove(computeArrayName(name));
                MapDBArray.removeArray(db, arrayID);
            });
        } finally {
            tx.close();
        }
    }

    @Override
    public <T extends Serializable> Array<T> getArray(String name, Class<T> cl) {
        if (!containsArrayName(name))
            return null;

        DB tx = sm.provider.tx();
        try {
            return DBUtils.atomicGet(tx, MAX_NUM_RETRIES, db -> {
                HTreeMap<String, Long> mapStorages = db.getHashMap(ARRAY_PREFIX+storageID);
                long arrayID = mapStorages.get(computeArrayName(name));
                return new MapDBArray(this, arrayID);
            });
        } finally {
            tx.close();
        }
    }

    @Override
    public List<String> getMatrixNames() {
        return null;
    }

    @Override
    public boolean containsMatrixName(String name) {
        return false;
    }

    @Override
    public <T extends Serializable> Matrix<T> createMatrix(String name, Class<T> cl, long numRows, long numCols) {
        return null;
    }

    @Override
    public void removeMatrix(String name) {

    }

    @Override
    public <T extends Serializable> Matrix<T> getMatrix(String name, Class<T> cl) {
        return null;
    }

    @Override
    public List<String> getDictionaryNames() {
        return null;
    }

    @Override
    public boolean containsDictionaryName(String name) {
        return false;
    }

    @Override
    public Dictionary createDictionary(String name) {
        return null;
    }

    @Override
    public void removeDictionary(String name) {

    }

    @Override
    public Dictionary getDictionary(String name) {
        return null;
    }

    @Override
    public List<String> getDataStreamNames() {
        return null;
    }

    @Override
    public boolean containsDataStreamName(String name) {
        return false;
    }

    @Override
    public DataStream createDataStream(String name) {
        return null;
    }

    @Override
    public void removeDataStream(String name) {

    }

    @Override
    public DataStream getDataStream(String name) {
        return null;
    }

    @Override
    public void flushData() {

    }


    static void removeStorage(DB db, long storageID) {

    }
}
