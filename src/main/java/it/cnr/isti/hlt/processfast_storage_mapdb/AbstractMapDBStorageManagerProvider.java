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

import it.cnr.isti.hlt.processfast.data.StorageManagerProvider;
import it.cnr.isti.hlt.processfast.utils.Function1;
import it.cnr.isti.hlt.processfast.utils.Procedure0;
import it.cnr.isti.hlt.processfast.utils.Procedure1;
import org.mapdb.DB;
import org.mapdb.TxMaker;

/**
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 */
public abstract class AbstractMapDBStorageManagerProvider implements StorageManagerProvider {

    /**
     * The MapDB db instance manager.
     */
    protected TxMaker txMaker;

    public TxMaker getTxMaker() {
        return txMaker;
    }


    /**
     * Create a new transaction to use to perform operations.
     *
     * @return A new transaction to perform operations.
     */
    public DB tx() {
        DB tx = txMaker.makeTx();
        return tx;
    }


    TxMaker txMaker() {
        return txMaker;
    }
}
