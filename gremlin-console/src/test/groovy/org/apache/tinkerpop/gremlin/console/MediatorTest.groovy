/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.console

import org.junit.Test

import static org.junit.Assert.assertEquals
import static org.junit.Assert.assertFalse
import static org.junit.Assert.assertNotNull
import static org.junit.Assert.assertTrue

/**
 * Unit tests for {@link Mediator} that avoid a live Gremlin server or a fully constructed {@link Console}.
 */
class MediatorTest {

    @Test
    void shouldConstructWithNullConsole() {
        final Mediator mediator = new Mediator(null)
        assertNotNull(mediator)
        assertTrue(mediator.availablePlugins.isEmpty())
        assertFalse(mediator.evaluating.get())
    }

    @Test
    void shouldReturnEmptyActivePluginsWhenNoneAvailable() {
        final Mediator mediator = new Mediator(null)
        assertTrue(mediator.activePlugins().isEmpty())
    }

    @Test
    void shouldReadEmptyPluginStateWhenConfigMissing() {
        // with no plugin-config file present, readPluginState() returns an empty list (not null)
        new File(ConsoleFs.PLUGIN_CONFIG_FILE).delete()
        assertEquals([], Mediator.readPluginState())
    }

    @Test
    void shouldNotWritePluginStateWhenConfigNotWritable() {
        // File.canWrite() is false when the config file does not exist, so writePluginState() reports false
        // and writes nothing
        new File(ConsoleFs.PLUGIN_CONFIG_FILE).delete()
        final Mediator mediator = new Mediator(null)
        assertFalse(mediator.writePluginState())
    }
}
