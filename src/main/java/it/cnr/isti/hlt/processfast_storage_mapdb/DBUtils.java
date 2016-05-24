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

import it.cnr.isti.hlt.processfast.utils.Function1;
import it.cnr.isti.hlt.processfast.utils.Procedure1;
import org.mapdb.DB;
import org.mapdb.TxMaker;

/**
 * Created by Tiziano on 10/06/2015.
 */
public class DBUtils {

    /**
     * Perform atomic operations on a single DB transaction. If atomic operations failed,
     * it will retry to perform the operations specified in "code" at maximum "maxNumRetries"
     * times.
     *
     * @param txMaker       The DB connection.
     * @param maxNumRetries The maximum number of tries to perform.
     * @param code          The code to be executed atomically.
     */
    public static void atomic(TxMaker txMaker, int maxNumRetries, Procedure1<DB> code) {
        if (txMaker == null)
            throw new NullPointerException("The txMaker object is 'null'");
        if (maxNumRetries < 0)
            throw new IllegalArgumentException("The maximum number of retries is less than 0: " + maxNumRetries);
        if (code == null)
            throw new NullPointerException("The code to execute is 'null'");

        Exception lastException = null;
        for (int i = 0; i < maxNumRetries; i++) {
            DB db = txMaker.makeTx();
            try {
                code.call(db);
                db.commit();
                return;
            } catch (Exception e) {
                db.rollback();
                lastException = e;
            } finally {
                db.close();
            }
        }

        throw new RuntimeException("Unable to perform transaction correctly", lastException);
    }


    /**
     * Perform atomic operations on a single DB transaction. If atomic operations failed,
     * it will retry to perform the operations specified in "code" at maximum "maxNumRetries"
     * times. The function upon successful execution of the code will return a proper result.
     *
     * @param txMaker The DB connection.
     * @param maxNumRetries The maximum number of tries to perform.
     * @param code The code to be executed atomically.
     * @return The result from execution of code.
     */
    public static <Out> Out atomicGet(TxMaker txMaker, int maxNumRetries, Function1<DB, Out> code) {
        if (txMaker == null)
            throw new NullPointerException("The txMaker object is 'null'");
        if (code == null)
            throw new NullPointerException("The code to execute is 'null'");

        Exception lastException = null;
        for (int i = 0; i < maxNumRetries; i++) {
            DB db = txMaker.makeTx();
            try {
                Out out = code.call(db);
                db.commit();
                return out;
            } catch (Exception e) {
                db.rollback();
                lastException = e;
            } finally {
                db.close();
            }
        }

        throw new RuntimeException("Unable to perform transaction correctly", lastException);
    }
}
