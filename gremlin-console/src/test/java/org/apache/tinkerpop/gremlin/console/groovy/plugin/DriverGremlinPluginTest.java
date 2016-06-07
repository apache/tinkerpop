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
package org.apache.tinkerpop.gremlin.console.groovy.plugin;

import org.codehaus.groovy.tools.shell.Groovysh;
import org.codehaus.groovy.tools.shell.IO;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class DriverGremlinPluginTest {

    @Test
    public void shouldConstructEmptyRemoteAcceptorWhenNotInConsoleEnvironment() throws Exception {
        final DriverGremlinPlugin plugin = new DriverGremlinPlugin();
        final SpyPluginAcceptor spy = new SpyPluginAcceptor();
        plugin.pluginTo(spy);

        assertThat(plugin.remoteAcceptor().isPresent(), is(false));
    }

    @Test
    public void shouldConstructRemoteAcceptorWhenInConsoleEnvironment() throws Exception {
        final DriverGremlinPlugin plugin = new DriverGremlinPlugin();
        final Map<String, Object> env = new HashMap<>();
        env.put("ConsolePluginAcceptor.io", new IO());
        env.put("ConsolePluginAcceptor.shell", new Groovysh());
        final SpyPluginAcceptor spy = new SpyPluginAcceptor(() -> env);
        plugin.pluginTo(spy);

        assertThat(plugin.remoteAcceptor().isPresent(), is(true));
    }
}
