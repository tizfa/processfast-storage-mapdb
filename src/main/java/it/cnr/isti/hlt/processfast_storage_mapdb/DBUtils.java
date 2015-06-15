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
    /*public static void atomic(DB db, int maxNumRetries, Procedure1<DB> code) {
        Exception lastException = null;
        for (int i = 0; i < maxNumRetries; i++) {
            try {
                code.call(db);
                db.commit();
                return;
            } catch (Exception e) {
                db.rollback();
                lastException = e;
            }
        }

        throw new RuntimeException("Unable to perform transaction correctly", lastException);
    }*/

    public static void atomic(TxMaker txMaker, int maxNumRetries, Procedure1<DB> code) {
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

    /*public static <Out> Out atomicGet(DB db, int maxNumRetries, Function1<DB, Out> code) {
        Exception lastException = null;
        for (int i = 0; i < maxNumRetries; i++) {
            try {
                Out out = code.call(db);
                db.commit();
                return out;
            } catch (Exception e) {
                db.rollback();
                lastException = e;
            }
        }

        throw new RuntimeException("Unable to perform transaction correctly", lastException);
    }*/


    public static <Out> Out atomicGet(TxMaker txMaker, int maxNumRetries, Function1<DB, Out> code) {
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
