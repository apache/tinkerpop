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

import org.junit.Test;

/**
 * Validates the {@code KdcFixture} behavior a bit - tests for a tests.
 */
public class KdcFixtureTest {

    @Test
    public void shouldCloseCleanly() throws Exception {
        final KdcFixture kdc;
        final String moduleBaseDir = System.getProperty("basedir");
        final String authConfigName = moduleBaseDir + "/src/test/resources/org/apache/tinkerpop/gremlin/server/gremlin-console-jaas.conf";
        System.setProperty("java.security.auth.login.config", authConfigName);
        kdc = new KdcFixture(moduleBaseDir);
        kdc.setUp();

        // deleting the principals will force an exception when they are deleted a second time on close
        kdc.deletePrincipals();

        // expect a clean shutdown despite failures that will come from trying to delete principals that aren't there
        kdc.close();
    }
}
