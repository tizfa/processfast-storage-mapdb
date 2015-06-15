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
import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by Tiziano on 09/06/2015.
 */
public class MapDBStorage implements Storage {

    private static final String ARRAY_PREFIX = "storage_arrays_";
    private static final String ARRAY_NUM_KEYS = "storage_arrays_keys";
    private static final String ARRAY_KEY_PREFIX = "storage_array_k_";
    private static final int MAX_NUM_RETRIES = 10;

    MapDBStorageManager sm;
    private final long storageID;
    private String storageName;

    public MapDBStorage(MapDBStorageManager sm, String storageName, long storageID) {
        if (sm == null)
            throw new NullPointerException("The storage manager is 'null'");
        if (storageName == null || storageName.isEmpty())
            throw new IllegalArgumentException("The storage name is 'null' or empty");
        this.sm = sm;
        this.storageID = storageID;
        this.storageName = storageName;

        createInitialStructures();
    }

    private void createInitialStructures() {
        // Create array structure, if not available.
        DBUtils.atomic(sm.provider.txMaker(), MAX_NUM_RETRIES, db -> {
            if (!db.exists(ARRAY_PREFIX + storageID)) {
                HTreeMap<String, Long> map = db.createHashMap(ARRAY_PREFIX + storageID).counterEnable().makeOrGet();
            }
        });

    }

    @Override
    public String getName() {
        return storageName;
    }

    @Override
    public List<String> getArrayNames() {

        ArrayList<String> toRet = DBUtils.atomicGet(sm.provider.txMaker(), MAX_NUM_RETRIES, db -> {
            return getArrayNames(db, storageID);
        });
        return toRet;

    }

    protected static ArrayList<String> getArrayNames(DB db, long storageID) {
        HTreeMap<String, Long> mapStorages = db.getHashMap(ARRAY_PREFIX + storageID);
        Iterator<String> keys = mapStorages.keySet().iterator();
        ArrayList<String> ret = new ArrayList<>();
        while (keys.hasNext()) {
            String key = keys.next();
            if (key.equals(ARRAY_NUM_KEYS))
                continue;
            ret.add(key);
        }
        return ret;
    }


    @Override
    public boolean containsArrayName(String name) {
        if (name == null || name.isEmpty())
            throw new IllegalArgumentException("The name is 'null' or empty");

        return DBUtils.atomicGet(sm.provider.txMaker(), MAX_NUM_RETRIES, db -> {
            return containsArrayName(db, name);
        });
    }


    public boolean containsArrayName(DB db, String name) {
        if (name == null || name.isEmpty())
            throw new IllegalArgumentException("The name is 'null' or empty");

        HTreeMap<String, Long> mapStorages = db.getHashMap(ARRAY_PREFIX + storageID);
        return mapStorages.containsKey(computeArrayName(name));

    }

    private String computeArrayName(String name) {
        return ARRAY_KEY_PREFIX + name;
    }

    @Override
    public <T extends Serializable> Array<T> createArray(String name, Class<T> cl) {
        if (name == null || name.isEmpty())
            throw new IllegalArgumentException("The name is 'null' or empty");

        long arrayIdx = DBUtils.atomicGet(sm.provider.txMaker(), MAX_NUM_RETRIES, db -> {
            HTreeMap<String, Long> mapArrays = db.getHashMap(ARRAY_PREFIX + storageID);
            long idx = 0;
            if (containsArrayName(db, name)) {
                idx = mapArrays.get(computeArrayName(name));
            } else {
                long nextID = 0;
                mapArrays.putIfAbsent(ARRAY_NUM_KEYS, 0l);
                nextID = mapArrays.get(ARRAY_NUM_KEYS);
                mapArrays.put(computeArrayName(name), nextID);
                mapArrays.put(ARRAY_NUM_KEYS, nextID + 1);
                idx = nextID;
            }
            return idx;
        });

        return new MapDBArray<T>(this, name, arrayIdx);
    }

    @Override
    public void removeArray(String name) {
        if (!containsArrayName(computeArrayName(name)))
            return;

        DBUtils.atomic(sm.provider.txMaker(), MAX_NUM_RETRIES, db -> {
            HTreeMap<String, Long> mapArrays = db.getHashMap(ARRAY_PREFIX + storageID);
            long arrayID = mapArrays.get(computeArrayName(name));
            mapArrays.remove(computeArrayName(name));
            MapDBArray.removeArray(db, storageID, arrayID);
        });
    }

    @Override
    public <T extends Serializable> Array<T> getArray(String name, Class<T> cl) {
        if (!containsArrayName(name))
            return null;

        long arrayID = DBUtils.atomicGet(sm.provider.txMaker(), MAX_NUM_RETRIES, db -> {
            HTreeMap<String, Long> mapStorages = db.getHashMap(ARRAY_PREFIX + storageID);
            return mapStorages.get(computeArrayName(name));
        });

        return new MapDBArray(this, name, arrayID);
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

        /**
         * Remove all arrays.
         */
        HTreeMap<String, Long> mapArrays = db.getHashMap(ARRAY_PREFIX + storageID);
        Iterator<String> keys = mapArrays.keySet().iterator();
        while (keys.hasNext()) {
            String key = keys.next();
            long arrayID = mapArrays.get(key);
            MapDBArray.removeArray(db, storageID, arrayID);
        }
        db.delete(ARRAY_PREFIX + storageID);
    }

    public long getStorageID() {
        return storageID;
    }
}
