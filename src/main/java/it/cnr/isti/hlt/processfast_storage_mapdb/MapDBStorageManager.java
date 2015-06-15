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

import it.cnr.isti.hlt.processfast.data.Storage;
import it.cnr.isti.hlt.processfast.data.StorageManager;
import org.mapdb.DB;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 *
 */
public class MapDBStorageManager implements StorageManager {
    AbstractMapDBStorageManagerProvider provider;

    private static final String STORAGE_TABLE_NAME = "st_storages";
    private static final String NUM_STORAGES_KEY = "st_key_NUM_STORAGES";
    private static final String STORAGE_KEY_PREFIX = "st_name_key_";
    private static final int MAX_NUM_RETRIES = 10;


    @Override
    public List<String> getStorageNames() {
        ArrayList<String> toRet = DBUtils.atomicGet(provider.txMaker(), MAX_NUM_RETRIES, db -> {
            HTreeMap<String, Long> mapStorages = db.getHashMap(STORAGE_TABLE_NAME);
            Iterator<String> keys = mapStorages.keySet().iterator();
            ArrayList<String> ret = new ArrayList<>();
            while (keys.hasNext()) {
                String key = keys.next();
                if (key.equals(NUM_STORAGES_KEY))
                    continue;
                ret.add(key);
            }
            return ret;
        });
        return toRet;
    }

    public MapDBStorageManager(AbstractMapDBStorageManagerProvider provider) {
        if (provider == null)
            throw new NullPointerException("The storage manager provider is 'null'");
        this.provider = provider;

        DBUtils.atomic(provider.txMaker(), 1, db -> {
            HTreeMap<String, Long> mapStorages = db.createHashMap(STORAGE_TABLE_NAME)
                    .keySerializer(Serializer.STRING)
                    .valueSerializer(Serializer.LONG)
                    .makeOrGet();
        });
    }

    @Override
    public boolean containsStorageName(String name) {
        if (name == null || name.isEmpty())
            throw new IllegalArgumentException("The name is 'null' or empty");

        return DBUtils.atomicGet(provider.txMaker(), MAX_NUM_RETRIES, db -> {
            HTreeMap<String, Long> mapStorages = db.getHashMap(STORAGE_TABLE_NAME);
            return mapStorages.containsKey(computeStorageName(name));
        });
    }

    public boolean containsStorageName(DB db, String name) {
        if (name == null || name.isEmpty())
            throw new IllegalArgumentException("The name is 'null' or empty");

        HTreeMap<String, Long> mapStorages = db.getHashMap(STORAGE_TABLE_NAME);
        return mapStorages.containsKey(computeStorageName(name));

    }

    private String computeStorageName(String name) {
        return STORAGE_KEY_PREFIX + name;
    }

    @Override
    public Storage createStorage(String name) {
        if (name == null || name.isEmpty())
            throw new IllegalArgumentException("The name is 'null' or empty");

        long storageIdx = DBUtils.atomicGet(provider.txMaker(), MAX_NUM_RETRIES, db -> {
            HTreeMap<String, Long> mapStorages = db.getHashMap(STORAGE_TABLE_NAME);
            long idx = 0;
            if (containsStorageName(db, name)) {
                idx = mapStorages.get(computeStorageName(name));
            } else {
                long nextID = 0;
                mapStorages.putIfAbsent(NUM_STORAGES_KEY, nextID);
                nextID = mapStorages.get(NUM_STORAGES_KEY);
                mapStorages.put(computeStorageName(name), nextID);
                idx = nextID;
            }
            return idx;
        });

        return new MapDBStorage(this, name, storageIdx);
    }

    @Override
    public void removeStorage(String name) {
        if (!containsStorageName(computeStorageName(name)))
            return;

        DBUtils.atomic(provider.txMaker(), MAX_NUM_RETRIES, db -> {
            HTreeMap<String, Long> mapStorages = db.getHashMap(STORAGE_TABLE_NAME);
            long storageID = mapStorages.get(computeStorageName(name));
            mapStorages.remove(computeStorageName(name));
            MapDBStorage.removeStorage(db, storageID);
        });
    }

    @Override
    public Storage getStorage(String name) {
        if (!containsStorageName(name))
            return null;

        return DBUtils.atomicGet(provider.txMaker(), MAX_NUM_RETRIES, db -> {
            HTreeMap<String, Long> mapStorages = db.getHashMap(STORAGE_TABLE_NAME);
            long storageID = mapStorages.get(computeStorageName(name));
            return new MapDBStorage(this, name, storageID);
        });

    }

    @Override
    public void flushData() {

    }

    @Override
    public void clear() {
        DBUtils.atomic(provider.txMaker(), MAX_NUM_RETRIES, db -> {
            HTreeMap<String, Long> mapStorages = db.getHashMap(STORAGE_TABLE_NAME);
            Iterator<String> keys = mapStorages.keySet().iterator();
            ArrayList<String> toRemove = new ArrayList<>();
            while (keys.hasNext())
                toRemove.add(keys.next());
            for (String key : toRemove) {
                long storageID = mapStorages.get(key);
                mapStorages.remove(key);
                MapDBStorage.removeStorage(db, storageID);
            }
        });
    }
}
