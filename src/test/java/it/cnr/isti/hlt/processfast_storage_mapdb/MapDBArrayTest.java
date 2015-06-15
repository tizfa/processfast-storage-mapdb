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

import it.cnr.isti.hlt.processfast.data.AbstractArrayTest;
import it.cnr.isti.hlt.processfast.data.Array;
import it.cnr.isti.hlt.processfast.data.Storage;
import it.cnr.isti.hlt.processfast.data.StorageManager;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 */
public class MapDBArrayTest extends AbstractArrayTest {

    private static MapDBRamStorageManagerProvider provider;

    @Override
    protected Array<Double> initArray(String name, boolean clearStorageData) {
        StorageManager sm = provider.getStorageManager("clientID");
        if (clearStorageData)
            sm.clear();
        Storage storage = sm.createStorage("test");
        return storage.createArray(name, Double.class);
    }

    /**
     * Init storage manager provider. Called one time before running any
     * test methods.
     */
    @BeforeClass
    public static void openStorageManagerProvider() {
        provider = new MapDBRamStorageManagerProvider();
        //provider.setStorageType(MapDBRamStorageManagerProvider.MapDBRamStorageType.MEMORY_DIRECT_DB);
        provider.open();
    }

    /**
     * Close storage manager provider. Called one time after have been executed all
     * defined test methods.
     */
    @AfterClass
    public static void closeStorageManagerProvider() {
        provider.close();
    }
}
