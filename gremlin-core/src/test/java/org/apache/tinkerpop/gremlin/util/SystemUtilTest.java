/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.tinkerpop.gremlin.util;

import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class SystemUtilTest {

    @Test
    public void shouldLoadSystemProperties() {
        System.setProperty("blah.aa", "1");
        System.setProperty("blah.b", "true");
        System.setProperty("blah.c", "three");
        System.setProperty("bleep.d", "false");
        Configuration configuration = SystemUtil.getSystemPropertiesConfiguration("blah", false);
        assertEquals(3, IteratorUtils.count(configuration.getKeys()));
        assertEquals(1, configuration.getInt("blah.aa"));
        assertTrue(configuration.getBoolean("blah.b"));
        assertEquals("three", configuration.getProperty("blah.c"));
        assertFalse(configuration.containsKey("d") || configuration.containsKey("bleep.d"));
        System.clearProperty("blah.aa");
        System.clearProperty("blah.b");
        System.clearProperty("blah.c");
        System.clearProperty("bleep.d");
    }

    @Test
    public void shouldTrimSystemPropertyPrefixes() {
        System.setProperty("blah.a", "1");
        System.setProperty("blah.bbb", "true");
        System.setProperty("blah.c", "three");
        System.setProperty("bleep.d", "false");
        Configuration configuration = SystemUtil.getSystemPropertiesConfiguration("blah", true);
        assertEquals(3, IteratorUtils.count(configuration.getKeys()));
        assertEquals(1, configuration.getInt("a"));
        assertTrue(configuration.getBoolean("bbb"));
        assertEquals("three", configuration.getProperty("c"));
        assertFalse(configuration.containsKey("d") || configuration.containsKey("bleep.d"));
        System.clearProperty("blah.a");
        System.clearProperty("blah.bbb");
        System.clearProperty("blah.c");
        System.clearProperty("bleep.d");
    }

    @Test
    public void shouldTrimSystemPropertyPrefixesAndNoMore() {
        System.setProperty("blah.a.x", "1");
        System.setProperty("blah.b.y", "true");
        System.setProperty("blah.cc.zzz", "three");
        System.setProperty("bleep.d.d", "false");
        Configuration configuration = SystemUtil.getSystemPropertiesConfiguration("blah", true);
        assertEquals(3, IteratorUtils.count(configuration.getKeys()));
        assertEquals(1, configuration.getInt("a.x"));
        assertTrue(configuration.getBoolean("b.y"));
        assertEquals("three", configuration.getProperty("cc.zzz"));
        assertFalse(configuration.containsKey("d") || configuration.containsKey("bleep.d"));
        assertFalse(configuration.containsKey("d.d") || configuration.containsKey("bleep.d.d"));
        System.clearProperty("blah.a.x");
        System.clearProperty("blah.b.y");
        System.clearProperty("blah.cc.zzz");
        System.clearProperty("bleep.d.d");
    }
}
