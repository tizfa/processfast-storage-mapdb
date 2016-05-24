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

import it.cnr.isti.hlt.processfast.data.Matrix;
import org.mapdb.Atomic;
import org.mapdb.BTreeMap;
import org.mapdb.DB;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 */
public class MapDBMatrix<T extends Serializable> implements Matrix<T> {

    private static final String MATRIX_PREFIX_ROW = "storage_matrix_row_";
    private static final String MATRIX_PREFIX_COL = "storage_matrix_col_";
    private static final int MAX_NUM_RETRIES = 10;
    private static final String NUM_ROWS_STORED_PREFIX = "mat_num_rows_stored_";
    private static final String NUM_COLS_STORED_PREFIX = "mat_num_cols_stored_";
    private static final String DEFAULT_VALUE_PREFIX = "mat_default_value_";
    private static final String KEY_PREFIX = "k_";
    private static final String NEXT_AVAILABLE_ROW_ID_PREFIX = "mat_next_available_rowid_";


    private final long matrixID;
    private final MapDBStorage storage;
    private final String name;

    public MapDBMatrix(MapDBStorage storage, String name, long matrixID) {
        if (storage == null)
            throw new NullPointerException("The parent storage is 'null'");
        if (name == null || name.isEmpty())
            throw new IllegalArgumentException("The name is 'null' or empty");

        this.storage = storage;
        this.name = name;
        this.matrixID = matrixID;

        // Create array structure, if not available.
        DBUtils.atomic(storage.sm.provider.txMaker(), MAX_NUM_RETRIES, db -> {
            if (!db.exists(getInternalMapRowName())) {
                BTreeMap<Long, Long> map = db.createTreeMap(getInternalMapRowName()).counterEnable().makeOrGet();
                db.createAtomicLong(getInternalNumRowsStored(), 1);
                db.createAtomicLong(getInternalNumColsStored(), 1);
                db.createAtomicLong(nextAvailableRowIDName(), 0);
            }
        });
    }

    protected String getInternalMapRowName() {
        return getInternalMapRowName(storage.getStorageID(), matrixID);
    }

    protected static String getInternalMapRowName(long storageID, long matrixID) {
        return MATRIX_PREFIX_ROW + storageID + "_" + matrixID;
    }

    protected String getInternalMapColName(long row) {
        return getInternalMapColName(storage.getStorageID(), matrixID, row);
    }

    protected static String getInternalMapColName(long storageID, long matrixID, long row) {
        return MATRIX_PREFIX_COL + storageID + "_" + matrixID + "_" + row;
    }


    protected String nextAvailableRowIDName() {
        return nextAvailableRowIDName(storage.getStorageID(), matrixID);
    }

    protected static String nextAvailableRowIDName(long storageID, long matrixID) {
        return NEXT_AVAILABLE_ROW_ID_PREFIX + storageID + "_" + matrixID;
    }

    protected String getInternalNumRowsStored() {
        return getInternalNumRowsStored(storage.getStorageID(), matrixID);
    }

    protected static String getInternalNumRowsStored(long storageID, long matrixID) {
        return NUM_ROWS_STORED_PREFIX + storageID + "_" + matrixID;
    }

    protected String getInternalNumColsStored() {
        return getInternalNumColsStored(storage.getStorageID(), matrixID);
    }

    protected static String getInternalNumColsStored(long storageID, long matrixID) {
        return NUM_COLS_STORED_PREFIX + storageID + "_" + matrixID;
    }

    protected static String getDefaultValueName(long storageID, long matrixID) {
        return DEFAULT_VALUE_PREFIX + storageID + "_" + matrixID;
    }

    protected String getDefaultValueName() {
        return getDefaultValueName(storage.getStorageID(), matrixID);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public long getNumCols() {
        return DBUtils.atomicGet(storage.sm.provider.txMaker(), MAX_NUM_RETRIES, db -> {
            return getNumCols(db);
        });
    }


    protected long getNumCols(DB db) {
        Atomic.Long num = db.getAtomicLong(getInternalNumColsStored());
        return num.get();
    }

    @Override
    public long getNumRows() {
        return DBUtils.atomicGet(storage.sm.provider.txMaker(), MAX_NUM_RETRIES, db -> {
            return getNumRows(db);
        });
    }

    protected long getNumRows(DB db) {
        Atomic.Long num = db.getAtomicLong(getInternalNumRowsStored());
        return num.get();
    }

    @Override
    public void resize(long numRows, long numColumns) {
        if (numRows < 1)
            throw new IllegalArgumentException("The number of rows is less than 1: " + numRows);
        if (numColumns < 1)
            throw new IllegalArgumentException("The number of cols is less than 1: " + numColumns);
        DBUtils.atomic(storage.sm.provider.txMaker(), MAX_NUM_RETRIES, db -> {
            resize(db, numRows, numColumns);
        });
    }


    protected String computeKey(long row, long col) {
        return KEY_PREFIX + row + "_" + col;
    }

    protected void resize(DB db, long numRows, long numCols) {
        BTreeMap<Long, Long> map = db.getTreeMap(getInternalMapRowName());
        Iterator<Long> keys = map.keySet().iterator();
        // Delete all rows associated with this matrix.
        while (keys.hasNext()) {
            long key = keys.next();
            long rowID = map.get(key);
            db.delete(getInternalMapColName(rowID));
        }
        map.clear();
        db.getAtomicLong(getInternalNumRowsStored()).set(numRows);
        db.getAtomicLong(getInternalNumColsStored()).set(numCols);
    }

    @Override
    public T getValue(long row, long column) {
        return DBUtils.atomicGet(storage.sm.provider.txMaker(), MAX_NUM_RETRIES, db -> {
            return getValue(db, row, column);
        });
    }

    protected T getValue(DB db, long row, long col) {
        long numCols = getNumCols(db);
        long numRows = getNumRows(db);
        if (row < 0 || row >= numRows)
            throw new IllegalArgumentException("The row index is not valid: " + row);
        if (col < 0 || col >= numCols)
            throw new IllegalArgumentException("The column index is not valid: " + col);

        BTreeMap<Long, Long> map = db.getTreeMap(getInternalMapRowName());
        if (!map.containsKey(row)) {
            Atomic.Long nextID = db.getAtomicLong(nextAvailableRowIDName());
            long nextIDAssigned = nextID.getAndIncrement();
            map.put(row, nextIDAssigned);
            db.createTreeMap(getInternalMapColName(nextIDAssigned)).counterEnable().makeOrGet();
        }
        long rowID = map.get(row);
        BTreeMap<Long, T> mapRow = db.getTreeMap(getInternalMapColName(rowID));
        if (mapRow.containsKey(col)) {
            return mapRow.get(col);
        } else {
            return getDefaultValue(db);
        }
    }


    @Override
    public void setValue(long row, long column, T value) {
        DBUtils.atomic(storage.sm.provider.txMaker(), MAX_NUM_RETRIES, db -> {
            setValue(db, row, column, value);
        });
    }

    protected void setValue(DB db, long row, long col, T value) {
        long numCols = getNumCols(db);
        long numRows = getNumRows(db);
        if (row < 0 || row >= numRows)
            throw new IllegalArgumentException("The row index is not valid: " + row);
        if (col < 0 || col >= numCols)
            throw new IllegalArgumentException("The column index is not valid: " + col);


        BTreeMap<Long, Long> map = db.getTreeMap(getInternalMapRowName());
        if (!map.containsKey(row)) {
            Atomic.Long nextID = db.getAtomicLong(nextAvailableRowIDName());
            long nextIDAssigned = nextID.getAndIncrement();
            map.put(row, nextIDAssigned);
            db.createTreeMap(getInternalMapColName(nextIDAssigned)).counterEnable().makeOrGet();
        }
        long rowID = map.get(row);
        BTreeMap<Long, T> mapRow = db.getTreeMap(getInternalMapColName(rowID));
        if (value != null)
            mapRow.put(col, value);
        else
            mapRow.remove(col);
    }


    @Override
    public void setDefaultValue(T value) {
        DBUtils.atomic(storage.sm.provider.txMaker(), MAX_NUM_RETRIES, db -> {
            Atomic.Var<T> val = db.getAtomicVar(getDefaultValueName());
            val.set(value);
        });
    }

    @Override
    public T getDefaultValue() {
        return DBUtils.atomicGet(storage.sm.provider.txMaker(), MAX_NUM_RETRIES, db -> {
            return getDefaultValue(db);
        });
    }

    protected T getDefaultValue(DB db) {
        Atomic.Var<T> val = db.getAtomicVar(getDefaultValueName());
        return val.get();
    }

    @Override
    public List<T> getRowValues(long row, long startCol, long endCol) {
        return DBUtils.atomicGet(storage.sm.provider.txMaker(), MAX_NUM_RETRIES, db -> {
            return getRowValues(db, row, startCol, endCol);
        });
    }


    protected List<T> getRowValues(DB db, long row, long startCol, long endCol) {
        if (startCol < 0)
            throw new IllegalArgumentException("The starting column is invalid: " + startCol);
        if (startCol >= endCol)
            throw new IllegalArgumentException("The starting column is greater equals to end column: startCol: " + startCol + ", endCol: " + endCol);
        long numRows = getNumRows(db);
        long numCols = getNumCols(db);
        if (row < 0 || row >= numRows)
            throw new IllegalArgumentException("The row index is not valid: " + row);
        ArrayList<T> values = new ArrayList<>();
        for (long j = startCol; j < endCol; j++) {
            values.add(getValue(db, row, j));
        }
        return values;
    }

    @Override
    public List<T> getColValues(long col, long startRow, long endRow) {
        return DBUtils.atomicGet(storage.sm.provider.txMaker(), MAX_NUM_RETRIES, db -> {
            return getColValues(db, col, startRow, endRow);
        });
    }

    protected List<T> getColValues(DB db, long col, long startRow, long endRow) {
        if (startRow < 0)
            throw new IllegalArgumentException("The starting row is invalid: " + startRow);
        if (startRow >= endRow)
            throw new IllegalArgumentException("The starting row is greater equals to end row: startRow: " + startRow + ", endRow: " + endRow);
        long numRows = getNumRows(db);
        long numCols = getNumCols(db);
        if (col < 0 || col >= numCols)
            throw new IllegalArgumentException("The column index is not valid: " + col);
        ArrayList<T> values = new ArrayList<>();
        for (long j = startRow; j < endRow; j++) {
            values.add(getValue(db, j, col));
        }
        return values;
    }

    @Override
    public void enableLocalCache(boolean enabled, long fromRowIndex, long toRowIndex, long fromColumnIndex, long toColumnIndex) {

    }

    @Override
    public boolean isLocalCacheEnabled(long row, long col) {
        return false;
    }

    @Override
    public void flush() {
    }

    public static void removeMatrix(DB db, long storageID, long matrixID) {
        BTreeMap<Long, Long> map = db.getTreeMap(getInternalMapRowName(storageID, matrixID));
        Iterator<Long> keys = map.keySet().iterator();
        while (keys.hasNext()) {
            long key = keys.next();
            long rowID = map.get(key);
            db.delete(getInternalMapColName(storageID, matrixID, rowID));
        }
        db.delete(getInternalMapRowName(storageID, matrixID));
        db.delete(getInternalNumRowsStored(storageID, matrixID));
        db.createAtomicLong(getInternalNumColsStored(storageID, matrixID), 1);
        db.createAtomicLong(nextAvailableRowIDName(storageID, matrixID), 0);
    }
}
