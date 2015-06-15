package it.cnr.isti.hlt.processfast_storage_mapdb;

import it.cnr.isti.hlt.processfast.data.Matrix;

import java.io.Serializable;
import java.util.List;

/**
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 */
public class MapDBMatrix<T extends Serializable> implements Matrix<T> {
    @Override
    public String getName() {
        return null;
    }

    @Override
    public long getNumCols() {
        return 0;
    }

    @Override
    public long getNumRows() {
        return 0;
    }

    @Override
    public void resize(long numRows, long numColumns) {

    }

    @Override
    public T getValue(long row, long column) {
        return null;
    }

    @Override
    public void setValue(long row, long column, T value) {

    }

    @Override
    public void setDefaultValue(T value) {

    }

    @Override
    public T getDefaultValue() {
        return null;
    }

    @Override
    public List<T> getRowValues(long row, long startCol, long endCol) {
        return null;
    }

    @Override
    public List<T> getColValues(long col, long startCol, long endCol) {
        return null;
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
}
