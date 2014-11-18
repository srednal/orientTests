package com.srednal.orientdb.tests;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.iterator.ORecordIteratorClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.junit.*;

import java.util.List;

import static org.junit.Assert.*;

/**
 * https://github.com/orientechnologies/orientdb/issues/2743
 *
 * A document query using both ORDER BY and SKIP requires LIMIT (or else it returns an empty collection).
 */
public class LimitSkipTest {

    ODatabaseDocumentTx db;

    @Before
    public void beforeEach() {
        db = new ODatabaseDocumentTx("plocal:build/databases/limitSkipTest");

        // ensure db exists and is open
        if (!db.exists()) db.create();
        if (db.isClosed()) db.open("admin", "admin");

        // ensure class exists
        OSchema schema = db.getMetadata().getSchema();
        if (!schema.existsClass("alphabet")) {
            schema.createClass("alphabet");
        }

        // remove old data
        ORecordIteratorClass<ODocument> iter = db.browseClass("alphabet");
        while (iter.hasNext()) {
            iter.next().delete();
        }

        // add 26 entries: { "letter": "A", "number": 0 }, ... { "letter": "Z", "number": 25 }

        String rowModel = "{\"letter\": \"%s\", \"number\": %d}";
        for (int i = 0; i < 26; ++i) {
            String l = String.valueOf((char) ('A' + i));
            String json = String.format(rowModel, l, i);
            ODocument doc = db.newInstance("alphabet");
            doc.fromJSON(json);
            doc.save();
        }
    }

    @After
    public void afterEach() {
        db.close();
    }


    ///////////////////////////////////////////////////////////////////////////////////

    // Some basic tests to ensure data got created as expected and that queries work
    // This let me copy/paste the second set of tests (with ORDER BY) and not change the basic test structure or assertions

    @Test
    public void testBasicCount() {
        assertEquals("count", 26, db.countClass("alphabet"));
    }

    @Test
    public void testBasicQuery() {
        OSQLSynchQuery sql = new OSQLSynchQuery("SELECT from alphabet");
        List<ODocument> results = db.query(sql);
        assertEquals("size", 26, results.size());
    }

    @Test
    public void testSkipZero() {
        OSQLSynchQuery sql = new OSQLSynchQuery("SELECT from alphabet SKIP 0");
        List<ODocument> results = db.query(sql);
        assertEquals("size", 26, results.size());
    }

    @Test
    public void testSkip() {
        OSQLSynchQuery sql = new OSQLSynchQuery("SELECT from alphabet SKIP 7");
        List<ODocument> results = db.query(sql);
        assertEquals("size", 19, results.size());
    }

    @Test
    public void testLimit() {
        OSQLSynchQuery sql = new OSQLSynchQuery("SELECT from alphabet LIMIT 9");
        List<ODocument> results = db.query(sql);
        assertEquals("size", 9, results.size());
    }

    @Test
    public void testLimitMinusOne() {
        OSQLSynchQuery sql = new OSQLSynchQuery("SELECT from alphabet LIMIT -1");
        List<ODocument> results = db.query(sql);
        assertEquals("size", 26, results.size());
    }

    @Test
    public void testSkipAndLimit() {
        OSQLSynchQuery sql = new OSQLSynchQuery("SELECT from alphabet SKIP 7 LIMIT 9");
        List<ODocument> results = db.query(sql);
        assertEquals("size", 9, results.size());
    }

    @Test
    public void testSkipAndLimitMinusOne() {
        OSQLSynchQuery sql = new OSQLSynchQuery("SELECT from alphabet SKIP 7 LIMIT -1");
        List<ODocument> results = db.query(sql);
        assertEquals("size", 19, results.size());
    }

    /////////////////////////////////////////////////////////////////////////////////////

    // Tests with ORDER BY
    // Failures occur when ORDER BY and SKIP are used without LIMIT
    // LIMIT is required when both ORDER BY and SKIP are specified

    @Test
    public void testBasicQueryOrdered() {
        OSQLSynchQuery sql = new OSQLSynchQuery("SELECT from alphabet ORDER BY letter");
        List<ODocument> results = db.query(sql);
        assertEquals("size", 26, results.size());
    }

    @Test
    public void testSkipZeroOrdered() {
        OSQLSynchQuery sql = new OSQLSynchQuery("SELECT from alphabet ORDER BY letter SKIP 0");
        List<ODocument> results = db.query(sql);
        assertEquals("size", 26, results.size());
    }

    @Test
    public void testSkipOrdered() {
        OSQLSynchQuery sql = new OSQLSynchQuery("SELECT from alphabet ORDER BY letter SKIP 7");
        List<ODocument> results = db.query(sql);
        assertEquals("size", 19, results.size());  // FAILURE - actual 0
    }

    @Test
    public void testLimitOrdered() {
        OSQLSynchQuery sql = new OSQLSynchQuery("SELECT from alphabet ORDER BY letter LIMIT 9");
        List<ODocument> results = db.query(sql);
        assertEquals("size", 9, results.size());
    }

    @Test
    public void testLimitMinusOneOrdered() {
        OSQLSynchQuery sql = new OSQLSynchQuery("SELECT from alphabet ORDER BY letter LIMIT -1");
        List<ODocument> results = db.query(sql);
        assertEquals("size", 26, results.size());
    }

    @Test
    public void testSkipAndLimitOrdered() {
        OSQLSynchQuery sql = new OSQLSynchQuery("SELECT from alphabet ORDER BY letter SKIP 7 LIMIT 9");
        List<ODocument> results = db.query(sql);
        assertEquals("size", 9, results.size());
    }

    @Test
    public void testSkipAndLimitMinusOneOrdered() {
        OSQLSynchQuery sql = new OSQLSynchQuery("SELECT from alphabet ORDER BY letter SKIP 7 LIMIT -1");
        List<ODocument> results = db.query(sql);
        assertEquals("size", 19, results.size());  // FAILURE - actual 0
    }
}
