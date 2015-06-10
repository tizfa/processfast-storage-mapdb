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

import java.io.Serializable;
import java.util.List;

/**
 * Created by Tiziano on 09/06/2015.
 */
public class MapDBStorage implements Storage {

    private MapDBStorageManager sm;
    private long storageID;

    public MapDBStorage(MapDBStorageManager sm, long storageID) {
        if (sm == null)
            throw new NullPointerException("The storage manager is 'null'");
        this.sm = sm;
        this.storageID = storageID;
    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    public List<String> getArrayNames() {
        return null;
    }

    @Override
    public boolean containsArrayName(String name) {
        return false;
    }

    @Override
    public <T extends Serializable> Array<T> createArray(String name, Class<T> cl) {
        return null;
    }

    @Override
    public void removeArray(String name) {

    }

    @Override
    public <T extends Serializable> Array<T> getArray(String name, Class<T> cl) {
        return null;
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
