package it.cnr.isti.hlt.processfast_storage_mapdb;

import it.cnr.isti.hlt.processfast.data.StorageManagerProvider;
import org.mapdb.DB;

/**
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 */
public abstract class AbstractMapDBStorageManagerProvider implements StorageManagerProvider {

    /**
     * The MapDB db instance manager.
     */
    protected DB db;


    /**
     * Get the internal used MapDB db instance.
     *
     * @return The internal used MapDB db instance.
     */
    public DB getDb() {
        return db;
    }
}
