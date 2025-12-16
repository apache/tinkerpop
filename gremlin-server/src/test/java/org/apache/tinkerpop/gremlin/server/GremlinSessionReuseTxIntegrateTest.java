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
package org.apache.tinkerpop.gremlin.server;

import org.apache.tinkerpop.gremlin.driver.Cluster;

/**
 * Integration tests for gremlin-driver and bytecode sessions where the underlying connection can be re-used for
 * multiple sessions. The server is configured with "closeSessionPostGraphOp" set to True.
 */
public class GremlinSessionReuseTxIntegrateTest extends AbstractSessionTxIntegrateTest {

    @Override
    protected Cluster createCluster() {
        return TestClientFactory.build().create();
    }

    /**
     * Configure specific Gremlin Server settings for specific tests.
     */
    @Override
    public Settings overrideSettings(final Settings settings) {
        super.overrideSettings(settings);

        // This setting allows connection re-use on the server side.
        settings.closeSessionPostGraphOp = true;

        return settings;
    }
}
