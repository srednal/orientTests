package com.srednal.orientdb.tests;


import com.orientechnologies.orient.core.db.document.ODatabaseDocumentPool;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import org.junit.Before;
import org.junit.Test;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;


/**
 * https://github.com/orientechnologies/orientdb/issues/3075
 * 
 * OrientDB encounters a deadlock when multiple threads do schema.createTable concurrently with transaction commit.
 * This test creates a condition where multiple threads are banging away at table creation and transactional operations.
 * It usually deadlocks between threads doing the create and those doing commit.
 * <p/>
 * It appears likely that locks are being obtained out-of-order between these two code paths.
 */
public class DeadlockTest {
    String localUser = "admin";
    String localUserPasswd = "admin";
    String dbPath = "build/db/deadlockTest";

    String dbUri = "plocal:" + dbPath;

    int tableCount = 15;
    int threadsPerTable = 5;
    int executorPoolSize = 10;

    @Before
    public void beforeEach() {
        // clear things out initially
        ODatabaseDocumentTx db = new ODatabaseDocumentTx(dbUri);
        try {
            if (db.exists()) {
                if (db.isClosed()) db.open(localUser, localUserPasswd);
                db.drop();
            }
            db.create();
        } finally {
            db.close();
        }
    }

    class Runner implements Runnable {
        private final String table;

        Runner(String table) {
            this.table = table;
        }

        public void run() {
            ODatabaseDocumentTx db = ODatabaseDocumentPool.global().acquire(dbUri, localUser, localUserPasswd);
            try {
                // create the table if needed
                try {
                    OSchema schema = db.getMetadata().getSchema();
                    if (!schema.existsClass(table)) {
                        try {  // there is a race condition between exists and create, but we really don't care
                            schema.createClass(table);
//                            System.out.println(table + " created");
                        } catch (Exception ignored) {
                            ignored.printStackTrace();
                        }
                    }

                    // and do some transactional stuff
                    db.begin();
                    try {
//                        System.out.println(table + " updating");
                        // delete stuff
                        new OCommandSQL("DELETE FROM " + table + " WHERE key = ?").execute(table);
                        // insert stuff
                        db.newInstance(table).fromJSON("{ \" key \": \"" + table + "\", \"aint\": 1, \"bstr\": \"" + table + "\" }").save();
                        db.newInstance(table).fromJSON("{ \" key \": \"" + table + "\", \"aint\": 2, \"bstr\": \"bar\" }").save();
                        // query stuff
                        new OCommandSQL("SELECT FROM " + table + " WHERE key = ?").execute(table);
                        // commit stuff
                        db.commit();

                    } catch (Exception e) {
                        db.rollback();
                        throw e;
                    }
                } finally {
                    db.close();
                }
            } catch (Exception ignored) {
                ignored.printStackTrace();
            }
        }
    }

    @Test
    public void testDeadlock() throws Exception {
        List<String> tables = new ArrayList<>();
        for (int i = 0; i < tableCount; ++i) {
            for (int j = 0; j < threadsPerTable; ++j) {
                tables.add("stuff." + i);
            }
        }
        Collections.shuffle(tables);

        ExecutorService pool = Executors.newFixedThreadPool(executorPoolSize);
        for (String table : tables) {
            pool.submit(new Runner(table));
        }
        pool.shutdown();
        boolean timedOut = !pool.awaitTermination(45, TimeUnit.SECONDS);


        ThreadMXBean tmbean = ManagementFactory.getThreadMXBean();
        long[] deadlocked = tmbean.findDeadlockedThreads();
        if (deadlocked != null) {
            StringBuilder errors = new StringBuilder();
            for (long id : deadlocked) {
                ThreadInfo info = tmbean.getThreadInfo(id);
                errors.append(info);
            }
            assertNull("deadlock: " + errors, deadlocked);
        }
        assertFalse("timeout", timedOut);
    }
}
