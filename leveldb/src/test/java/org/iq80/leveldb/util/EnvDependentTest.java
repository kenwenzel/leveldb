/*
 * Copyright (C) 2011 the original author or authors. See the notice.md file distributed with this
 * work for additional information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.iq80.leveldb.util;

import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;

import org.iq80.leveldb.Env;
import org.iq80.leveldb.Env.DBHandle;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

public abstract class EnvDependentTest implements EnvTestProvider {
  private Env env;
  private DBHandle handle;

  @BeforeMethod
  public void setupEnvAndDB() throws Exception {
    final Entry<? extends Env, ? extends DBHandle> envAndDB = createTempDB();
    this.env = envAndDB.getKey();
    this.handle = envAndDB.getValue();
  }

  @AfterMethod
  public void tearDownDB() throws InterruptedException, ExecutionException {
    getEnv().deleteDB(getHandle()).toCompletableFuture().get();
  }

  protected Env getEnv() {
    return env;
  }

  protected DBHandle getHandle() {
    return handle;
  }

}
