/*
 *
 * ****************
 * Copyright 2015 Tiziano Fagni (tiziano.fagni@isti.cnr.it)
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
 * ******************
 */

package it.cnr.isti.hlt.processfast_storage_mapdb;

import it.cnr.isti.hlt.processfast.data.StorageManager;
import org.mapdb.DBMaker;

/**
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 */
public class MapDBRamStorageManagerProvider extends AbstractMapDBStorageManagerProvider {

    public enum MapDBRamStorageType {
        /**
         * The data is stored all on heap without using serialization.
         */
        HEAP_DB,

        /**
         * The data is stored all on heap and using serialization.
         */
        MEMORY_DB,

        /**
         * The data is stored outside heap (no GC involved) and using serialization.
         */
        MEMORY_DIRECT_DB,
    }


    /**
     * The storage type to use while creating the DB.
     */
    private MapDBRamStorageType storageType = MapDBRamStorageType.HEAP_DB;


    @Override
    public StorageManager getStorageManager(String clientID) {
        return new MapDBStorageManager(this);
    }

    @Override
    public void open() {
        if (storageType == MapDBRamStorageType.MEMORY_DIRECT_DB)
            txMaker = DBMaker.newMemoryDirectDB().closeOnJvmShutdown().makeTxMaker();
        else if (storageType == MapDBRamStorageType.MEMORY_DB)
            txMaker = DBMaker.newMemoryDB().closeOnJvmShutdown().makeTxMaker();
        else
            txMaker = DBMaker.newHeapDB().closeOnJvmShutdown().makeTxMaker();
    }

    @Override
    public void close() {
        txMaker.close();
        txMaker = null;
    }

    public MapDBRamStorageType getStorageType() {
        return storageType;
    }

    public void setStorageType(MapDBRamStorageType storageType) {
        if (storageType == null)
            throw new NullPointerException("The storage type is 'null'");
        this.storageType = storageType;
    }
}
