package com.dawsonsystems.session;

import com.mongodb.*;
import org.apache.catalina.*;
import org.apache.catalina.session.ManagerBase;
import org.apache.catalina.session.StandardSession;
import org.apache.catalina.util.CustomObjectInputStream;

import java.io.*;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class MongoManager extends ManagerBase implements Lifecycle {
    private static Logger log = Logger.getLogger("MongoManager");
    protected final static String SESSION_ID_FIELD = "sessionid";
    protected static String host = "localhost";
    protected static int port = 27017;
    protected static String database = "sessions";
    protected static int expiryMinutes = 30;
    protected Mongo mongo;
    protected DB db;

    private MongoSessionTrackerValve trackerValve;

    public int getRejectedSessions() {
        return 0;
    }

    public void setRejectedSessions(int i) { }

    public void load() throws ClassNotFoundException, IOException { }

    public void unload() throws IOException {}

    public void addLifecycleListener(LifecycleListener lifecycleListener) {}

    public LifecycleListener[] findLifecycleListeners() {
        return new LifecycleListener[0];  //To change body of implemented methods use File | Settings | File Templates.
    }

    public void removeLifecycleListener(LifecycleListener lifecycleListener) {}

    public void start() throws LifecycleException {
        for(Valve valve : getContainer().getPipeline().getValves()) {
            if (valve instanceof MongoSessionTrackerValve) {
                trackerValve = (MongoSessionTrackerValve) valve;
                trackerValve.setMongoManager(this);
                log.info("Found Mongo Tracker Valve");
                break;
            }
        }
        initDbConnection();

        //TODO, an expiry task executor
    }

    public void stop() throws LifecycleException {
         mongo.close();
    }

    @Override
    public Session findSession(String id) throws IOException {
        //TODO, loadSession from MOngo
        return loadSession(id);
    }

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

    public Session loadSession(String id) throws IOException {
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
            Container container = getContainer();

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
            session = (StandardSession) createEmptySession();
            session.readObjectData(ois);
            session.setManager(this);
            log.info("Loaded session id " + id);
            return session;
        } catch (IOException e) {
            log.severe(e.getMessage());
            throw e;
        } catch (ClassNotFoundException ex) {
            throw new IOException ("Unable to deserialize session", ex);
        }
    }
//
//    public void remove(String id) throws IOException {
//        try {
//            log.info("Removing session " + id + " from Mongo");
//            BasicDBObject o = new BasicDBObject();
//            o.put(SESSION_ID_FIELD, id);
//            getCollection().remove(o);
//        } catch (IOException e) {
//            e.printStackTrace();
//            throw e;
//        }
//    }

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

    public void initDbConnection() throws LifecycleException {
        try {
            String[] hosts = getHost().split(",");

            List<ServerAddress> addrs = new ArrayList<ServerAddress>();

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
}
