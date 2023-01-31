/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.server.notifications;

import org.apache.tinkerpop.gremlin.TestHelper;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.RequestOptions;
import org.apache.tinkerpop.gremlin.util.Tokens;
import org.apache.tinkerpop.gremlin.server.AbstractGremlinServerIntegrationTest;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.server.TestClientFactory;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.Map;
import java.util.Random;
import java.util.UUID;

public class GraphManagerNotificationsTest extends AbstractGremlinServerIntegrationTest {

  @Override
  public Settings overrideSettings(final Settings settings) {
    settings.graphManager = "org.apache.tinkerpop.gremlin.server.notifications.ProviderGraphManagerHelper";
    return settings;
  }

  @Test
  public void scriptSuccessShouldNotifyGraphManager() throws Exception {
    final Cluster cluster = TestClientFactory.open();
    final Client client = cluster.connect(name.getMethodName());
    final Random random = TestHelper.RANDOM;
    final UUID requestID = new UUID(random.nextLong(), random.nextLong());
    final RequestOptions options = RequestOptions.build().overrideRequestId(requestID).create();

    final String script = "1+1";
    client.submit(script, options).all().get().get(0);

    final ProviderGraphManagerHelper graphManager = (ProviderGraphManagerHelper) server.getServerGremlinExecutor()
        .getGraphManager();
    final Map<String, Object> requestArgs = graphManager.getBeforeQueryStartTracking(requestID.toString());
    assertEquals(script, requestArgs.get(Tokens.ARGS_GREMLIN));
    assertThat(graphManager.didRequestSucceed(requestID.toString()), is(true));
    assertThat(graphManager.didRequestFail(requestID.toString()), is(false));
    cluster.close();
  }

  @Test
  public void scriptFailureShouldNotifyGraphManager() throws Exception {
    final Cluster cluster = TestClientFactory.open();
    final Client client = cluster.connect(name.getMethodName());
    final Random random = TestHelper.RANDOM;
    final UUID requestID = new UUID(random.nextLong(), random.nextLong());
    final RequestOptions options = RequestOptions.build().overrideRequestId(requestID).create();

    final String script = "x";
    try {
      client.submit(script, options).all().get().get(0);
      fail("This script should fail since the variable x is not defined");
    } catch (Exception e) {
      //
    }

    final ProviderGraphManagerHelper graphManager = (ProviderGraphManagerHelper) server.getServerGremlinExecutor()
        .getGraphManager();
    final Map<String, Object> requestArgs = graphManager.getBeforeQueryStartTracking(requestID.toString());
    assertEquals(script, requestArgs.get(Tokens.ARGS_GREMLIN));
    assertThat(graphManager.didRequestSucceed(requestID.toString()), is(false));
    assertThat(graphManager.didRequestFail(requestID.toString()), is(true));
    cluster.close();
  }

}