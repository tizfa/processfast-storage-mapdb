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
