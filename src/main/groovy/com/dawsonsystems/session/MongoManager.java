package com.dawsonsystems.session;

import com.mongodb.*;
import org.apache.catalina.*;
import org.apache.catalina.session.ManagerBase;
import org.apache.catalina.session.StandardSession;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
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
  protected boolean slaveOk;

  private MongoSessionTrackerValve trackerValve;
  private ThreadLocal<StandardSession> currentSession = new ThreadLocal<StandardSession>();
  private Serializer serializer;

  //Either 'kryo' or 'java'
  private String serializationStrategyClass = "com.dawsonsystems.session.JavaSerializer";

  public int getRejectedSessions() {
    return 0;
  }

  public void setSerializationStrategyClass(String strategy) {
    this.serializationStrategyClass = strategy;
  }

  public void setSlaveOk(boolean slaveOk) {
    this.slaveOk = slaveOk;
  }

  public boolean getSlaveOk() {
    return slaveOk;
  }

  public void setRejectedSessions(int i) {
  }

  public void load() throws ClassNotFoundException, IOException {
  }

  public void unload() throws IOException {
  }

  public void addLifecycleListener(LifecycleListener lifecycleListener) {
  }

  public LifecycleListener[] findLifecycleListeners() {
    return new LifecycleListener[0];  //To change body of implemented methods use File | Settings | File Templates.
  }

  public void removeLifecycleListener(LifecycleListener lifecycleListener) {
  }

  @Override
  public Session createEmptySession() {
    MongoSession session = new MongoSession(this);
    session.setMaxInactiveInterval(-1);
    currentSession.set(session);
    return session;
  }

  /**
   * @deprecated
   */
  public org.apache.catalina.Session createSession() {
    return createEmptySession();
  }

  public org.apache.catalina.Session createSession(java.lang.String sessionId) {
    StandardSession session = (MongoSession) createEmptySession();

    session.setId(sessionId);

    return session;
  }

  public org.apache.catalina.Session[] findSessions() {
    try {
      List<Session> sessions = new ArrayList<Session>();
      for(String sessionId : keys()) {
        sessions.add(loadSession(sessionId));
      }
      return sessions.toArray(new Session[sessions.size()]);
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  protected org.apache.catalina.session.StandardSession getNewSession() {
    return (MongoSession) createEmptySession();
  }

  public void start() throws LifecycleException {
    for (Valve valve : getContainer().getPipeline().getValves()) {
      if (valve instanceof MongoSessionTrackerValve) {
        trackerValve = (MongoSessionTrackerValve) valve;
        trackerValve.setMongoManager(this);
        log.info("Attached to Mongo Tracker Valve");
        break;
      }
    }
    try {
      initSerializer();
    } catch (ClassNotFoundException e) {
      log.log(Level.SEVERE, "Unable to load serializer", e);
      throw new LifecycleException(e);
    } catch (InstantiationException e) {
      log.log(Level.SEVERE, "Unable to load serializer", e);
      throw new LifecycleException(e);
    } catch (IllegalAccessException e) {
      log.log(Level.SEVERE, "Unable to load serializer", e);
      throw new LifecycleException(e);
    }
    log.info("Will expire sessions after " + getMaxInactiveInterval() + " seconds");
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

    while (cursor.hasNext()) {
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
      query.put("_id", id);

      DBObject dbsession = getCollection().findOne(query);

      if (dbsession == null) {
        log.fine("Session " + id + " not found in Mongo");
        StandardSession ret = getNewSession();
        ret.setId(id);
        currentSession.set(ret);
        return ret;
      }

      byte[] data = (byte[]) dbsession.get("data");

      session = (MongoSession) createEmptySession();
      session.setId(id);
      session.setManager(this);
      serializer.deserializeInto(data, session);

      session.setMaxInactiveInterval(-1);
      session.access();
      session.setValid(true);
      session.setNew(false);

      if (log.isLoggable(Level.FINE)) {
        log.fine("Session Contents [" + session.getId() + "]:");
        for (Object name : Collections.list(session.getAttributeNames())) {
          log.fine("  " + name);
        }
      }

      log.fine("Loaded session id " + id);
      currentSession.set(session);
      return session;
    } catch (IOException e) {
      log.severe(e.getMessage());
      throw e;
    } catch (ClassNotFoundException ex) {
      log.log(Level.SEVERE, "Unable to deserialize session ", ex);
      throw new IOException("Unable to deserializeInto session", ex);
    }
  }

  public void save(Session session) throws IOException {
    try {
      log.fine("Saving session " + session + " into Mongo");

      StandardSession standardsession = (MongoSession) session;

      if (log.isLoggable(Level.FINE)) {
        log.fine("Session Contents [" + session.getId() + "]:");
        for (Object name : Collections.list(standardsession.getAttributeNames())) {
          log.fine("  " + name);
        }
      }

      byte[] data = serializer.serializeFrom(standardsession);

      BasicDBObject dbsession = new BasicDBObject();
      dbsession.put("_id", standardsession.getId());
      dbsession.put("data", data);
      dbsession.put("lastmodified", System.currentTimeMillis());

      BasicDBObject query = new BasicDBObject();
      query.put("_id", standardsession.getIdInternal());
      getCollection().update(query, dbsession, true, false);
      log.fine("Updated session with id " + session.getIdInternal());
    } catch (IOException e) {
      log.severe(e.getMessage());
      e.printStackTrace();
      throw e;
    } finally {
      currentSession.remove();
      log.fine("Session removed from ThreadLocal :" + session.getIdInternal());
    }
  }

  public void remove(Session session) {
    log.fine("Removing session ID : " + session.getId());
    BasicDBObject query = new BasicDBObject();
    query.put("_id", session.getId());

    try {
      getCollection().remove(query);
    } catch (IOException e) {
      log.log(Level.SEVERE, "Error removing session in Mongo Session Store", e);
    } finally {
      currentSession.remove();
    }
  }

  public void processExpires() {
    BasicDBObject query = new BasicDBObject();

    long olderThan = System.currentTimeMillis() - (getMaxInactiveInterval() * 1000);

    log.fine("Looking for sessions less than for expiry in Mongo : " + olderThan);

    query.put("lastmodified", new BasicDBObject("$lt", olderThan));

    try {
      WriteResult result = getCollection().remove(query);
      log.fine("Expired sessions : " + result.getN());
    } catch (IOException e) {
      log.log(Level.SEVERE, "Error cleaning session in Mongo Session Store", e);
    }
  }

  private void initDbConnection() throws LifecycleException {
    try {
      String[] hosts = getHost().split(",");

      List<ServerAddress> addrs = new ArrayList<ServerAddress>();

      for (String host : hosts) {
        addrs.add(new ServerAddress(host, getPort()));
      }
      mongo = new Mongo(addrs);
      db = mongo.getDB(getDatabase());
      if (slaveOk) {
        db.slaveOk();
      }
      getCollection().ensureIndex(new BasicDBObject("lastmodified", 1));
      log.info("Connected to Mongo " + host + "/" + database + " for session storage, slaveOk=" + slaveOk + ", " + (getMaxInactiveInterval() * 1000) + " session live time");
    } catch (IOException e) {
      e.printStackTrace();
      throw new LifecycleException("Error Connecting to Mongo", e);
    }
  }

  private void initSerializer() throws ClassNotFoundException, IllegalAccessException, InstantiationException {
    log.info("Attempting to use serializer :" + serializationStrategyClass);
    serializer = (Serializer) Class.forName(serializationStrategyClass).newInstance();

    Loader loader = null;

    if (container != null) {
      loader = container.getLoader();
    }
    ClassLoader classLoader = null;

    if (loader != null) {
      classLoader = loader.getClassLoader();
    }
    serializer.setClassLoader(classLoader);
  }
}
