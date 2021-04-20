package edu.upenn.cis.stormlite.storage;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.Transaction;
import com.sleepycat.persist.EntityCursor;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import com.sleepycat.persist.StoreConfig;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DatabaseEnv {
    static Logger log = LogManager.getLogger(DatabaseEnv.class);

    Environment env;
    EntityStore store;
    PrimaryIndex<String, ReduceState> stateByKey;
    String directory;

    public DatabaseEnv(String dir) {
        log.info("Creating database environment for " + dir);

        directory = dir;
        if (!Files.exists(Paths.get(directory))) {
            try {
                Files.createDirectory(Paths.get(directory));
            } catch (IOException e) {
                log.error("Can't create database environment folder");
                log.error(e);
            }
        }

        try {
            EnvironmentConfig envConfig = new EnvironmentConfig();
            StoreConfig storeConfig = new StoreConfig();

            envConfig.setAllowCreate(true);
            envConfig.setTransactional(true);

            storeConfig.setAllowCreate(true);
            storeConfig.setTransactional(true);

            env = new Environment(new File(directory), envConfig);
            store = new EntityStore(env, "EntityStore", storeConfig);

            stateByKey = store.getPrimaryIndex(String.class, ReduceState.class);

        } catch (DatabaseException dbe) {
            log.error("Error opening environment and store");
            log.error(dbe);
        }
    }

    public EntityCursor<ReduceState> getStateCursor() {
        return stateByKey.entities();
    }

    public void addValue(String key, String value) {
        Transaction txn = env.beginTransaction(null, null);
        ReduceState state = stateByKey.get(key);

        if (state == null) {
            state = new ReduceState(key);
            log.info("Creating new reduce state for " + key);
        }

        state.addValue(value);
        stateByKey.put(state);
        txn.commit();
        log.info("Adding value to reduce state for " + key);
    }

    public void close() {
        store.truncateClass(ReduceState.class);

        if (store != null) {
            try {
                store.close();
            } catch (DatabaseException dbe) {
                log.error("Error closing store");
                log.error(dbe);
            }
        }
        if (env != null) {
            try {
                env.close();
            } catch (DatabaseException dbe) {
                log.error("Error closing database environment");
                log.error(dbe);
            }
        }
    }

    public void reset() {
        store.truncateClass(ReduceState.class);
        stateByKey = store.getPrimaryIndex(String.class, ReduceState.class);
    }

    public String toString() {

        // Print Reduce State.
        EntityCursor<ReduceState> state = stateByKey.entities();
        int count = 0;
        String res = "=======================================\n";

        for (ReduceState currState : state) {
            res += currState;
            count++;
        }
        state.close();
        res += "Number of reduce keys: " + count + "\n";
        res += "=======================================\n";

        return res;
    }

}
