package com.dawsonsystems.session;

import org.apache.catalina.Manager;
import org.apache.catalina.session.StandardSession;

public class MongoSession extends StandardSession {

  public MongoSession(Manager manager) {
    super(manager);
  }

  @Override
  protected boolean isValidInternal() {
    return isValid;
  }
}
