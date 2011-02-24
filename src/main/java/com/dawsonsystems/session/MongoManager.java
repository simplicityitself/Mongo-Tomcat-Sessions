package com.dawsonsystems.session;

import com.mongodb.*;
import org.apache.catalina.*;
import org.apache.catalina.session.ManagerBase;
import org.apache.catalina.session.StandardSession;
import org.apache.catalina.util.CustomObjectInputStream;
import org.bson.types.ObjectId;

import java.beans.PropertyChangeListener;
import java.io.*;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class MongoManager extends ManagerBase implements Lifecycle {
    private static Logger log = Logger.getLogger("MongoManager");
    protected static String host = "localhost";
    protected static int port = 27017;
    protected static String database = "sessions";
    protected Mongo mongo;
    protected DB db;

    private MongoSessionTrackerValve trackerValve;
    private ThreadLocal<StandardSession> currentSession = new ThreadLocal<StandardSession>();

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
                log.info("Attached to Mongo Tracker Valve");
                break;
            }
        }
        log.info ("Will expire sessions after " + getMaxInactiveInterval() + " seconds");
        initDbConnection();
    }

    public void stop() throws LifecycleException {
         mongo.close();
    }

    public Session findSession(String id) throws IOException {
        return loadSession(id);
    }

    public static String getHost() {
        return host;
    }

    public static void setHost(String host) {
        MongoManager.host = host;
    }

    public static int getPort() {
        return port;
    }

    public static void setPort(int port) {
        MongoManager.port = port;
    }

    public static String getDatabase() {
        return database;
    }

    public static void setDatabase(String database) {
        MongoManager.database = database;
    }

    public void clear() throws IOException {
        getCollection().drop();
        getCollection().ensureIndex(new BasicDBObject("lastmodified", 1));
    }

    private DBCollection getCollection() throws IOException {
        return db.getCollection("sessions");
    }

    public int getSize() throws IOException {
        return (int) getCollection().count();
    }

    public String[] keys() throws IOException {

        BasicDBObject restrict = new BasicDBObject();
        restrict.put("_id", 1);

        DBCursor cursor = getCollection().find(new BasicDBObject(), restrict);

        List<String> ret = new ArrayList<String>();

        while(cursor.hasNext()) {
            ret.add(cursor.next().get("").toString());
        }

        return ret.toArray(new String[ret.size()]);
    }

    public Session loadSession(String id) throws IOException {

        StandardSession session = currentSession.get();

        if (session != null) {
            if (id.equals(session.getId())) {
                return session;
            } else {
                currentSession.remove();
            }
        }
        try {
            log.fine("Loading session " + id + " from Mongo");
            BasicDBObject query = new BasicDBObject();
            query.put("_id", new ObjectId(id));

            DBObject dbsession = getCollection().findOne(query);

            if (dbsession == null) {
                log.fine("Session " + id + " not found in Mongo");
                StandardSession ret = getNewSession();
                ret.setId(id);
                currentSession.set(ret);
                return ret;
            }

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
            session.setId(id);
            session.readObjectData(ois);
            session.setManager(this);
            log.fine("Loaded session id " + id);
            currentSession.set(session);
            return session;
        } catch (IOException e) {
            log.severe(e.getMessage());
            throw e;
        } catch (ClassNotFoundException ex) {
            throw new IOException ("Unable to deserialize session", ex);
        }
    }

    public void save(Session session) throws IOException {
        try {
            log.fine("Saving session " + session + " into Mongo");

            ObjectOutputStream oos = null;
            ByteArrayOutputStream bos = null;

            bos = new ByteArrayOutputStream();
            oos = new ObjectOutputStream(new BufferedOutputStream(bos));

            StandardSession standardsession = (StandardSession) session;
            standardsession.writeObjectData(oos);

            oos.close();
            oos = null;

            BasicDBObject dbsession = new BasicDBObject();
            dbsession.put("_id", new ObjectId(standardsession.getIdInternal()));
            dbsession.put("data", bos.toByteArray());
            dbsession.put("lastmodified", System.currentTimeMillis());

            BasicDBObject query = new BasicDBObject();
            query.put("_id", new ObjectId(standardsession.getIdInternal()));
            getCollection().update(query, dbsession, true, false);
            log.fine("Updated session with id " + session.getIdInternal());
            currentSession.remove();
        } catch (IOException e) {
            log.severe(e.getMessage());
            e.printStackTrace();
            throw e;
        }
    }

    public void processExpires() {
        BasicDBObject query = new BasicDBObject();
        log.fine("Expiring Sessions in MONGO (DISABLED)");

        long olderThan = System.currentTimeMillis() - (getMaxInactiveInterval() * 1000);

        log.fine("Looking for sessions less than : " + olderThan);

        query.put("lastmodified", new BasicDBObject("$lt", olderThan));

        try {
            WriteResult result = getCollection().remove(query);
            log.fine("Expired sessions : " + result.getN());
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
            getCollection().ensureIndex(new BasicDBObject("lastmodified", 1));
            log.info("Connected to Mongo " + host + " for session storage, slaveOk, 30 minute session live time");
        } catch (IOException e) {
            e.printStackTrace();
            throw new LifecycleException("Error Connecting to Mongo", e);
        }
    }

    protected String generateSessionId() {
        return new ObjectId().toString();
    }

}
