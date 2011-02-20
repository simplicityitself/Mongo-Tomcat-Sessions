package com.dawsonsystems.session.test;

import com.dawsonsystems.session.MongoStore;
import com.mongodb.*;
import org.apache.catalina.LifecycleException;
import org.apache.catalina.Session;
import org.apache.catalina.core.StandardContext;
import org.apache.catalina.session.PersistentManager;
import org.apache.catalina.session.StandardSession;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.UnknownHostException;

public class MongoStoreTest extends Assert {
    private PersistentManager manager;
    private MongoStore store;

    private Mongo mongo;
    private DB db;

    @Before
    public void startUp() throws UnknownHostException, LifecycleException {
        MongoStore.setDatabase("sessions-test");
        MongoStore.setHost("localhost");

        manager = new PersistentManager();
        manager.setContainer(new StandardContext());
        store = new MongoStore();
        store.setManager(manager);
        store.start();

        mongo = new Mongo("localhost");
        db = mongo.getDB("sessions-test");
        db.getCollection("sessions").remove(new BasicDBObject());
    }

    @After
    public void shutdown() throws LifecycleException {
        store.stop();
    }

    @Test
    public void save() throws IOException, ClassNotFoundException {
        Session session = manager.createSession(null);
        store.save(session);

        DBCursor cursor = db.getCollection("sessions").find(new BasicDBObject());

        assertEquals(1, cursor.size());

        DBObject dbsession = cursor.next();
        assertNotNull(dbsession);
        assertEquals(session.getId(), dbsession.get("sessionid"));

        byte[] data = (byte[]) dbsession.get("data");

        assertNotNull(data);
    }

    @Test
    public void update() throws IOException, ClassNotFoundException {
        Session session = manager.createSession(null);
        ((StandardSession) session).setAttribute("rah", "firstvalue");

        store.save(session);

        Session loadedSession = store.load(session.getId());
        ((StandardSession) loadedSession).setAttribute("rah", "secondvalue");

        store.save(loadedSession);

        DBCursor cursor = db.getCollection("sessions").find(new BasicDBObject());
        assertEquals(1, cursor.size());

        DBObject dbsession = cursor.next();
        assertNotNull(dbsession);
        assertEquals(session.getId(), dbsession.get("sessionid"));
        byte[] data = (byte[]) dbsession.get("data");
        assertNotNull(data);

        loadedSession = store.load(session.getId());

        assertEquals("secondvalue", ((StandardSession) loadedSession).getAttribute("rah"));
    }

    @Test
    public void load() throws IOException, ClassNotFoundException {
        Session savedSession = manager.createSession(null);
        ((StandardSession) savedSession).setAttribute("foo", "bar");
        store.save(savedSession);

        Session loadedSession = store.load(savedSession.getId());

        assertNotNull(loadedSession);
        assertEquals(savedSession.getId(), loadedSession.getId());
        assertEquals("bar", ((StandardSession) loadedSession)
                .getAttribute("foo"));
    }
}
