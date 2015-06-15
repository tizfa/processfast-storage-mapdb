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

import java.io.File;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
- */
public class MapDBFileStorageManagerProvider extends AbstractMapDBStorageManagerProvider {

    /**
     * The db filename.
     */
    private String dbFilename;


    private boolean createIfNotExistant;


    public MapDBFileStorageManagerProvider(String dbFilename, boolean createIfNotExistant) {
        if (dbFilename == null || dbFilename.isEmpty())
            throw new IllegalArgumentException("The db filename is 'null' or empty");
        this.dbFilename = dbFilename;
        this.createIfNotExistant = createIfNotExistant;
    }


    @Override
    public StorageManager getStorageManager(String clientID) {
        return new MapDBStorageManager(this);
    }


    @Override
    public void open() {
        File f = new File(dbFilename);
        if (!f.exists() && !createIfNotExistant)
            throw new IllegalArgumentException("Can not open db at " + dbFilename + ". The file does not exist!");

        txMaker = DBMaker.newFileDB(f).closeOnJvmShutdown().makeTxMaker();
    }

    @Override
    public void close() {
       txMaker.close();
    }

}
