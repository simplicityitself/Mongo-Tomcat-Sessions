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

import org.apache.catalina.Manager;
import org.apache.catalina.session.StandardSession;

import java.util.logging.Logger;

public class MongoSession extends StandardSession {
  private static Logger log = Logger.getLogger("MongoManager");
  private boolean isValid = true;

  public MongoSession(Manager manager) {
    super(manager);
  }

  @Override
  protected boolean isValidInternal() {
    return isValid;
  }

  @Override
  public boolean isValid() {
    return isValidInternal();
  }

  @Override
  public void setValid(boolean isValid) {
    this.isValid = isValid;
    if (!isValid) {
      String keys[] = keys();
      for (String key : keys) {
        removeAttributeInternal(key, false);
      }
      getManager().remove(this);

    }
  }

  @Override
  public void invalidate() {
    setValid(false);
  }

  @Override
  public void setId(String id) {
    this.id = id;
  }
}
