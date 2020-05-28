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
package org.apache.tinkerpop.gremlin.groovy.util;

import org.apache.commons.io.FileUtils;
import org.apache.tinkerpop.gremlin.TestHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import groovy.lang.GroovyClassLoader;

import java.io.File;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author Jason Plurad (http://github.com/pluradj)
 */
public class DependencyGrabberIntegrateTest {
    private static final String GROUP_ID = "org.apache.tinkerpop";
    private static final String VERSION = "3.3.9";

    private static final GroovyClassLoader groovyClassLoader = new GroovyClassLoader();
    private static final File extTestDir = TestHelper.makeTestDataPath(DependencyGrabberIntegrateTest.class);
    private static final DependencyGrabber dg = new DependencyGrabber(groovyClassLoader, extTestDir);

    @Before
    public void setUp() {
        FileUtils.deleteQuietly(extTestDir);
    }

    @After
    public void tearDown() {
        FileUtils.deleteQuietly(extTestDir);
    }

    @Test
    public void shouldInstallAndUninstallDependencies() {
        final String name = "tinkergraph-gremlin";
        final Artifact a = new Artifact(GROUP_ID, name, VERSION);
        final File pluginDir = new File(extTestDir, name);

        // install the plugin
        dg.copyDependenciesToPath(a);
        assertTrue("Plugin installation directory exists", pluginDir.exists());

        // delete the plugin
        dg.deleteDependenciesFromPath(a);
        assertTrue("Plugin installation directory deleted", !pluginDir.exists());
    }

    @Test
    public void shouldThrowIllegalStateException() {
        final String name = "gremlin-groovy";
        final Artifact a = new Artifact(GROUP_ID, name, VERSION);
        final File pluginDir = new File(extTestDir, name);

        // install the plugin for the first time
        dg.copyDependenciesToPath(a);
        assertTrue("Plugin installation directory exists", pluginDir.exists());

        // attempt to install plugin a second time
        try {
            dg.copyDependenciesToPath(a);

            fail("The plugin re-installed");

        } catch (Exception ise) {
          assertTrue("Plugin re-install keeps the installation directory", pluginDir.exists());
        }
    }

    @Test
    public void shouldThrowRuntimeException() {
        final String name = "gremlin-bogus";
        final Artifact a = new Artifact(GROUP_ID, name, VERSION);
        final File pluginDir = new File(extTestDir, name);

        // attempt to install bogus plugin
        try {
            dg.copyDependenciesToPath(a);

            fail("The bogus plugin installed");

        } catch (Exception re) {
            assertTrue("The bogus plugin dir was not installed", !pluginDir.exists());
        }
    }
}
