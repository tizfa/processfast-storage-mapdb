package it.cnr.isti.hlt.processfast_storage_mapdb;

import it.cnr.isti.hlt.processfast.data.AbstractArrayTest;
import it.cnr.isti.hlt.processfast.data.Array;
import it.cnr.isti.hlt.processfast.data.Storage;
import it.cnr.isti.hlt.processfast.data.StorageManager;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 */
public class MapDBArrayTest extends AbstractArrayTest {

    private static MapDBRamStorageManagerProvider provider;

    @Override
    protected Array<Double> initArray(String name, boolean clearStorageData) {
        StorageManager sm = provider.getStorageManager("clientID");
        if (clearStorageData)
            sm.clear();
        Storage storage = sm.createStorage("test");
        return storage.createArray(name, Double.class);
    }

    /**
     * Init storage manager provider. Called one time before running any
     * test methods.
     */
    @BeforeClass
    public static void openStorageManagerProvider() {
        provider = new MapDBRamStorageManagerProvider();
        //provider.setStorageType(MapDBRamStorageManagerProvider.MapDBRamStorageType.MEMORY_DIRECT_DB);
        provider.open();
    }

    /**
     * Close storage manager provider. Called one time after have been executed all
     * defined test methods.
     */
    @AfterClass
    public static void closeStorageManagerProvider() {
        provider.close();
    }
}
