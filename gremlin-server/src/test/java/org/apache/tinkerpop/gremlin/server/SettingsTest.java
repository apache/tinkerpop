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
package org.apache.tinkerpop.gremlin.server;

import org.apache.tinkerpop.gremlin.util.TestSupport;
import org.junit.Test;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertEquals;

public class SettingsTest {

    private static InputStream getMinimalConfigStream() throws Exception {
        final File confDir = new File(TestSupport.getRootOfBuildDirectory(SettingsTest.class), "../conf");
        return new FileInputStream(new File(confDir, "gremlin-server-min.yaml"));
    }

    private static class CustomSettings extends Settings {
        public String customValue = "localhost";

        public static CustomSettings read(final InputStream stream) {
            final Constructor constructor = createDefaultYamlConstructor();
            final Yaml yaml = new Yaml(constructor);
            return yaml.loadAs(stream, CustomSettings.class);
        }
    }

    @Test
    public void constructorCanBeExtendToParseCustomYamlAndSettingsValues() throws Exception {
        final InputStream stream = SettingsTest.class.getResourceAsStream("custom-gremlin-server.yaml");

        final CustomSettings settings = CustomSettings.read(stream);

        assertEquals("hello", settings.customValue);
        assertEquals("remote", settings.host);
    }

    @Test
    public void defaultCustomValuesAreHandledCorrectly() throws Exception {
        final InputStream stream = SettingsTest.class.getResourceAsStream("gremlin-server-integration.yaml");

        final CustomSettings settings = CustomSettings.read(stream);

        assertEquals("localhost", settings.customValue);
    }

    @Test
    public void scriptEnginesDefaultsToGremlinLangWhenAbsentFromYaml() throws Exception {
        final InputStream stream = getMinimalConfigStream();
        final Settings settings = Settings.read(stream);

        assertThat(settings.scriptEngines, is(notNullValue()));
        assertThat(settings.scriptEngines.size(), is(1));
        assertThat(settings.scriptEngines, hasKey("gremlin-lang"));
    }

    @Test
    public void scriptEnginesPopulatedWhenPresentInYaml() throws Exception {
        final InputStream stream = SettingsTest.class.getResourceAsStream("gremlin-server-integration.yaml");
        final Settings settings = Settings.read(stream);

        assertThat(settings.scriptEngines, hasKey("gremlin-groovy"));
        assertThat(settings.scriptEngines, hasKey("gremlin-lang"));
    }

    @Test
    public void traversalSourcesParsedFromYaml() throws Exception {
        final InputStream stream = SettingsTest.class.getResourceAsStream("gremlin-server-with-traversal-sources.yaml");
        final Settings settings = Settings.read(stream);

        final Settings.GraphSettings graphSettings = settings.getGraphSettings("graph");
        assertThat(graphSettings, is(notNullValue()));
        assertThat(graphSettings.traversalSources.size(), is(2));

        final Settings.TraversalSourceSettings gSettings = graphSettings.traversalSources.get(0);
        assertThat(gSettings.name, is("g"));
        assertThat(gSettings.gremlinExpression, is(nullValue()));
        assertThat(gSettings.language, is(nullValue()));

        final Settings.TraversalSourceSettings roSettings = graphSettings.traversalSources.get(1);
        assertThat(roSettings.name, is("gReadOnly"));
        assertThat(roSettings.gremlinExpression, is("g.withStrategies(ReadOnlyStrategy)"));
        assertThat(roSettings.language, is("gremlin-groovy"));
    }

    @Test
    public void traversalSourcesDefaultsToEmptyListWhenAbsentFromYaml() throws Exception {
        final InputStream stream = getMinimalConfigStream();
        final Settings settings = Settings.read(stream);

        final Settings.GraphSettings graphSettings = settings.getGraphSettings("graph");
        assertThat(graphSettings, is(notNullValue()));
        assertThat(graphSettings.traversalSources, is(notNullValue()));
        assertThat(graphSettings.traversalSources.isEmpty(), is(true));
    }

    @Test
    public void lifecycleHooksParsedFromYaml() throws Exception {
        final InputStream stream = SettingsTest.class.getResourceAsStream("gremlin-server-with-traversal-sources.yaml");
        final Settings settings = Settings.read(stream);

        assertThat(settings.lifecycleHooks.size(), is(2));

        final Settings.LifeCycleHookSettings first = settings.lifecycleHooks.get(0);
        assertThat(first.className, is("org.apache.tinkerpop.gremlin.server.util.TinkerFactoryDataLoader"));
        assertThat(first.config, hasKey("graph"));
        assertThat(first.config.get("dataset"), is("modern"));

        final Settings.LifeCycleHookSettings second = settings.lifecycleHooks.get(1);
        assertThat(second.config.get("dataset"), is("classic"));
    }

    @Test
    public void lifecycleHooksDefaultsToEmptyListWhenAbsentFromYaml() throws Exception {
        final InputStream stream = getMinimalConfigStream();
        final Settings settings = Settings.read(stream);

        assertThat(settings.lifecycleHooks, is(notNullValue()));
        assertThat(settings.lifecycleHooks.isEmpty(), is(true));
    }

    @Test
    public void transactionTimeoutsDefaultToReasonableValuesWhenAbsentFromYaml() throws Exception {
        final Settings settings = Settings.read(getMinimalConfigStream());

        // Out of the box a transaction is bounded without any operator configuration: idle reclamation at 1 minute and
        // an absolute lifetime cap at 10 minutes.
        assertEquals(60000L, settings.idleTransactionTimeoutMillis);
        assertEquals(600000L, settings.maxTransactionLifetimeMillis);
    }

    @Test
    public void maxTransactionLifetimeParsedFromYaml() throws Exception {
        final InputStream stream = SettingsTest.class.getResourceAsStream("gremlin-server-integration.yaml");
        final Settings settings = Settings.read(stream);

        // Confirms a YAML-provided value overrides the code default (600000); the resource sets it to 480000.
        assertEquals(480000L, settings.maxTransactionLifetimeMillis);
    }
}
