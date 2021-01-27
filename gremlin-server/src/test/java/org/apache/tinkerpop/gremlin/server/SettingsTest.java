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
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.InputStream;

import static org.junit.Assert.assertEquals;

public class SettingsTest {

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
}
