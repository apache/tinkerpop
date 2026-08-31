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

import org.apache.tinkerpop.gremlin.util.ser.GraphSONMessageSerializerV1;
import org.apache.tinkerpop.shaded.jackson.databind.exc.InvalidTypeIdException;
import org.junit.Test;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.awt.Dimension;
import java.awt.Point;
import java.io.InputStream;
import java.util.Collections;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

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

    @Test
    public void shouldConfigureAllowedTypeIdNamesFromYaml() throws Exception {
        final GraphSONMessageSerializerV1 serializer = createGraphSONV1SerializerFromYaml();
        final String json = "{\"@class\":\"java.util.HashMap\",\"point\":{\"@class\":\"java.awt.Point\"," +
                "\"x\":1.0,\"y\":2.0}}";
        final Map<String, Object> deserialized = serializer.getMapper().readValue(json, Map.class);

        assertEquals(new Point(1, 2), deserialized.get("point"));
    }

    @Test
    public void shouldRejectUnregisteredTypeIdNamesFromYaml() {
        final GraphSONMessageSerializerV1 serializer = createGraphSONV1SerializerFromYaml();
        final String json = "{\"@class\":\"java.util.HashMap\",\"dimension\":{\"@class\":\"" +
                Dimension.class.getName() + "\",\"width\":1,\"height\":2}}";

        try {
            serializer.getMapper().readValue(json, Map.class);
            fail("an unregistered type id name should not resolve");
        } catch (Exception ex) {
            assertThat(ex, instanceOf(InvalidTypeIdException.class));
        }
    }

    private static GraphSONMessageSerializerV1 createGraphSONV1SerializerFromYaml() {
        final InputStream stream = SettingsTest.class.getResourceAsStream("gremlin-server-integration.yaml");
        final Settings settings = Settings.read(stream);
        final Settings.SerializerSettings serializerSettings = settings.serializers.stream()
                .filter(s -> GraphSONMessageSerializerV1.class.getName().equals(s.className))
                .findFirst().orElseThrow(IllegalStateException::new);
        final GraphSONMessageSerializerV1 serializer = new GraphSONMessageSerializerV1();
        serializer.configure(serializerSettings.config, Collections.emptyMap());
        return serializer;
    }
}
