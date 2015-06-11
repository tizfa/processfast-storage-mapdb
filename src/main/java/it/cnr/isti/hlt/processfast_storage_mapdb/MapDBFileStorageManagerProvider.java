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

        txMaker = DBMaker.newFileDB(f).closeOnJvmShutdown().transactionDisable().makeTxMaker();
    }

    @Override
    public void close() {
       txMaker.close();
    }

}
