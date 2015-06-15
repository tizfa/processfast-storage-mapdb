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
