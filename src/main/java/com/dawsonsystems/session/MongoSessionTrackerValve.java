/***********************************************************************************************************************
 *
 * Mongo Tomcat Sessions
 * ==========================================
 *
 * Copyright (C) 2012 by Dawson Systems Ltd (http://www.dawsonsystems.com)
 *
 ***********************************************************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package com.dawsonsystems.session;

import org.apache.catalina.Session;
import org.apache.catalina.connector.Request;
import org.apache.catalina.connector.Response;
import org.apache.catalina.valves.ValveBase;

import javax.servlet.ServletException;
import java.io.IOException;
import java.util.logging.Logger;


public class MongoSessionTrackerValve extends ValveBase {
  private static Logger log = Logger.getLogger("MongoSessionValve");
  private MongoManager manager;

  public void setMongoManager(MongoManager manager) {
    this.manager = manager;
  }

  @Override
  public void invoke(Request request, Response response) throws IOException, ServletException {
    try {
      getNext().invoke(request, response);
    } finally {
      storeSession(request, response);
    }
  }

  private void storeSession(Request request, Response response) throws IOException {
    final Session session = request.getSessionInternal(false);

    if (session != null) {
        if (session.isValid()) {
          log.fine("Request with session completed, saving session " + session.getId());
          if (session.getSession() != null) {
            log.fine("HTTP Session present, saving " + session.getId());
            manager.save(session);
          } else {
            log.fine("No HTTP Session present, Not saving " + session.getId());
          }
        } else {
            log.fine("HTTP Session has been invalidated, removing :" + session.getId());
            manager.remove(session);
        }
    }
  }
}
