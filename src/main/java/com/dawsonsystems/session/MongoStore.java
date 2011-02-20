package com.dawsonsystems.session;

import com.mongodb.*;
import org.apache.catalina.*;
import org.apache.catalina.session.StandardSession;
import org.apache.catalina.session.StoreBase;
import org.apache.catalina.util.CustomObjectInputStream;
import org.bson.types.ObjectId;

import java.io.*;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class MongoStore extends StoreBase {
    private static Logger log = Logger.getLogger("MongoStore");
    protected final static String SESSION_ID_FIELD = "sessionid";
    protected static String host = "localhost";
    protected static int port = 27017;
    protected static String database = "sessions";
    protected static int expiryMinutes = 30;
    protected Mongo mongo;
    protected DB db;
    
    public static String getHost() {
        return host;
    }

    public static void setHost(String host) {
        MongoStore.host = host;
    }

    public static int getPort() {
        return port;
    }

    public static void setPort(int port) {
        MongoStore.port = port;
    }

    public static String getDatabase() {
        return database;
    }

    public static void setDatabase(String database) {
        MongoStore.database = database;
    }

    public static int getExpiryMinutes() {
        return expiryMinutes;
    }

    public static void setExpiryMinutes(int expiryMinutes) {
        MongoStore.expiryMinutes = expiryMinutes;
    }

    public void clear() throws IOException {
        getCollection().drop();
        getCollection().createIndex(new BasicDBObject(SESSION_ID_FIELD, 1));
    }

    private DBCollection getCollection() throws IOException {
        return db.getCollection("sessions");
    }

    public int getSize() throws IOException {
        return (int) getCollection().count();
    }

    public String[] keys() throws IOException {

        BasicDBObject restrict = new BasicDBObject();
        restrict.put(SESSION_ID_FIELD, 1);

        DBCursor cursor = getCollection().find(new BasicDBObject(), restrict);

        List<String> ret = new ArrayList<String>();

        while(cursor.hasNext()) {
            ret.add((String) cursor.next().get(SESSION_ID_FIELD));
        }

        return ret.toArray(new String[ret.size()]);
    }

    public Session load(String id) throws ClassNotFoundException, IOException {
        try {
            log.info("Loading session " + id + " from Mongo");
            BasicDBObject query = new BasicDBObject();
            query.put(SESSION_ID_FIELD, id);

            DBObject dbsession = getCollection().findOne(query);

            if (dbsession == null) {
                log.info("Session " + id + " not found in Mongo");
                return null;
            }

            StandardSession session = null;

            ObjectInputStream ois;
            Container container = manager.getContainer();

            BufferedInputStream bis = new BufferedInputStream(
                    new ByteArrayInputStream((byte[]) dbsession.get("data")));
            Loader loader = null;
            if (container != null) {
                loader = container.getLoader();
            }
            ClassLoader classLoader = null;
            if (loader != null) {
                classLoader = loader.getClassLoader();
            }
            if (classLoader != null) {
                ois = new CustomObjectInputStream(bis, classLoader);
            } else {
                ois = new ObjectInputStream(bis);
            }
            session = (StandardSession) manager.createEmptySession();
            session.readObjectData(ois);
            session.setManager(manager);
            log.info("Loaded session id " + id);
            return session;
        } catch (IOException e) {
            log.severe(e.getMessage());
        }
        return null;
    }

    public void remove(String id) throws IOException {
        try {
            log.info("Removing session " + id + " from Mongo");
            BasicDBObject o = new BasicDBObject();
            o.put(SESSION_ID_FIELD, id);
            getCollection().remove(o);
        } catch (IOException e) {
            e.printStackTrace();
            throw e;
        }
    }

    public void save(Session session) throws IOException {
        try {
            log.info("Saving session " + session + " into Mongo");
            ObjectOutputStream oos = null;
            ByteArrayOutputStream bos = null;

            bos = new ByteArrayOutputStream();
            oos = new ObjectOutputStream(new BufferedOutputStream(bos));

            StandardSession standardsession = (StandardSession) session;
            standardsession.writeObjectData(oos);

            oos.close();
            oos = null;

            BasicDBObject dbsession = new BasicDBObject();
            dbsession.put(SESSION_ID_FIELD, standardsession.getIdInternal());
            dbsession.put("data", bos.toByteArray());
            dbsession.put("lastmodified", new Date());

            BasicDBObject query = new BasicDBObject();
            query.put(SESSION_ID_FIELD, standardsession.getIdInternal());
            getCollection().update(query, dbsession, true, false);
            log.info("Updated session with id " + session.getIdInternal());

        } catch (IOException e) {
            log.severe(e.getMessage());
            e.printStackTrace();
            throw e;
        }
    }

    @Override
    public void processExpires() {
        BasicDBObject query = new BasicDBObject();
        log.info("Expiring Sessions in MONGO");

        Date olderThan = new Date(System.currentTimeMillis() - expiryMinutes);
        query.put("lastmodified", new BasicDBObject("$lt", olderThan));

        try {
            WriteResult result = getCollection().remove(query);
            log.info("Expired sessions : " + result.getN());
        } catch (IOException e) {
            log.log(Level.SEVERE, "Error cleaning session in Mongo Session Store", e);
        }
    }

    @Override
    public void start() throws LifecycleException {
        super.start();
        try {
            String[] hosts = getHost().split(",");

            List<ServerAddress> addrs = new ArrayList();

            for (String host : hosts) {
                addrs.add( new ServerAddress( host , getPort() ) );
            }
            mongo = new Mongo(addrs);
            db = mongo.getDB(getDatabase());
            db.slaveOk();
            log.info("Connected to Mongo " + host + " for session storage, slaveOk, 30 minute session live time");
        } catch (UnknownHostException e) {
            e.printStackTrace();
            throw new LifecycleException("Error Connecting to Mongo", e);
        }
    }

    @Override
    public void stop() throws LifecycleException {
        mongo.close();
    }
}
