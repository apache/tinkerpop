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
package org.apache.tinkerpop.gremlin.console.jsr223;

import org.apache.tinkerpop.gremlin.TestHelper;
import org.apache.tinkerpop.gremlin.jsr223.console.RemoteException;
import org.apache.tinkerpop.gremlin.structure.io.Storage;
import org.codehaus.groovy.tools.shell.Groovysh;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

import static org.hamcrest.CoreMatchers.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class DriverRemoteAcceptorTest {

    private final Groovysh groovysh = new Groovysh();
    private DriverRemoteAcceptor acceptor;

    @Before
    public void setUp() {
        acceptor = new DriverRemoteAcceptor(new MockGroovyGremlinShellEnvironment(groovysh));
    }

    @After
    public void tearDown() {
        try {
            acceptor.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @Test(expected = RemoteException.class)
    public void shouldNotConfigureWithBadCommand() throws Exception {
        acceptor.configure(Arrays.asList("test"));
    }

    @Test(expected = RemoteException.class)
    public void shouldNotConfigureWithUnevenPairsOfAliases() throws Exception {
        acceptor.configure(Arrays.asList("alias g social x"));
    }

    @Test
    public void shouldResetAliases() throws Exception {
        final Map<String,String> resetAliases = (Map<String,String>) acceptor.configure(Arrays.asList("alias", "g", "main.social"));
        assertEquals(1, resetAliases.size());
        assertEquals("main.social", resetAliases.get("g"));

        assertEquals("Aliases cleared", acceptor.configure(Arrays.asList("alias", "reset")));

        final Map<String,String> shownAliases = (Map<String,String>) acceptor.configure(Arrays.asList("alias", "show"));
        assertEquals(0, shownAliases.size());
    }

    @Test
    public void shouldAddOverwriteAndShowAliases() throws Exception {
        final Map<String,String> aliases = (Map<String,String>) acceptor.configure(Arrays.asList("alias", "g", "social", "graph", "main"));
        assertEquals(2, aliases.size());
        assertEquals("social", aliases.get("g"));
        assertEquals("main", aliases.get("graph"));

        final Map<String,String> resetAliases = (Map<String,String>) acceptor.configure(Arrays.asList("alias", "g", "main.social"));
        assertEquals(1, resetAliases.size());
        assertEquals("main.social", resetAliases.get("g"));

        final Map<String,String> shownAliases = (Map<String,String>) acceptor.configure(Arrays.asList("alias", "show"));
        assertEquals(1, shownAliases.size());
        assertEquals("main.social", shownAliases.get("g"));
    }

    @Test(expected = RemoteException.class)
    public void shouldNotConnectWithEmptyArgs() throws Exception {
        acceptor.connect(new ArrayList<>());
    }

    @Test(expected = RemoteException.class)
    public void shouldNotConnectWithTooManyArgs() throws Exception {
        acceptor.connect(Arrays.asList("two", "too", "many"));
    }

    @Test(expected = RemoteException.class)
    public void shouldNotConnectWithInvalidConfigFile() throws Exception {
        acceptor.connect(Arrays.asList("this-isnt-real.yaml"));
    }

    @Test
    public void shouldConfigureTimeoutToNone() throws Exception {
        acceptor.configure(Arrays.asList("timeout", "none"));
        assertEquals(DriverRemoteAcceptor.NO_TIMEOUT, acceptor.getTimeout());
    }

    @Test
    public void shouldConfigureTimeout() throws Exception {
        acceptor.configure(Arrays.asList("timeout", "123456"));
        assertEquals(123456, acceptor.getTimeout());
    }

    @Test(expected = RemoteException.class)
    public void shouldConfigureTimeoutNotLessThanNoTimeout() throws Exception {
        acceptor.configure(Arrays.asList("timeout", "-1"));
    }

    @Test
    public void shouldConnectWithError() throws Exception {
        // there is no gremlin server running for this test, so the driver will throw NoHostAvailableException, log an
        // error, and return with a message to the console.
        assertThat(acceptor.connect(Arrays.asList(Storage.toPath(TestHelper.generateTempFileFromResource(this.getClass(),
                        "remote.yaml", ".tmp")))).toString(), startsWith("Configured "));
    }
}
