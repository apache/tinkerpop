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
package org.apache.tinkerpop.gremlin.util.config;

import org.apache.commons.configuration.ConfigurationException;
import org.junit.Test;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;

import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class YamlConfigurationTest {

    @Test(expected = ConfigurationException.class)
    public void shouldThrowConfigurationExceptionIfLoadFails() throws Exception {
        final YamlConfiguration config = new YamlConfiguration();
        config.load((Reader) null);
    }

    @Test(expected = ConfigurationException.class)
    public void shouldThrowConfigurationExceptionIfSaveFails() throws Exception {
        final YamlConfiguration config = new YamlConfiguration();
        config.save((Writer) null);
    }

    @Test
    public void shouldLoadSaveConfiguration() throws Exception {
        final YamlConfiguration config = new YamlConfiguration();
        final String testData = this.getTestInputData();
        final StringWriter writer = new StringWriter();
        config.load(new StringReader(testData));
        config.save(writer);

        assertEquals(testData, writer.getBuffer().toString());
    }

    @Test
    public void shouldNavigationConfiguration() throws Exception {
        final Map<Object, Object> testData = this.getTestData();
        final YamlConfiguration config = new YamlConfiguration();
        config.load(new StringReader(this.getTestInputData()));

        assertEquals(testData.get("integer"), config.getInt("integer"));
        assertEquals(testData.get("string"), config.getString("string"));
        assertEquals(testData.get("long"), config.getLong("long"));
        assertEquals(testData.get("true-boolean"), config.getBoolean("true-boolean"));
        assertEquals(testData.get("false-boolean"), config.getBoolean("false-boolean"));
        assertEquals(testData.get("list"), config.getList("list.item"));
        assertEquals(testData.get("42"), config.getString("42"));

        final Map<Object, Object> subData = this.getSubMap();
        assertEquals(subData.get("sub-string"), config.getString("map.sub-string"));
        assertEquals("String1", config.getString("map.sub-list.item(1).item(0)"));
    }

    @Test
    public void shouldSupportXMLCompatibility() throws Exception {
        final YamlConfiguration config = new YamlConfiguration();
        config.setXmlCompatibility(true);
        config.load(new StringReader(this.getTestInputData()));
        assertEquals(Arrays.asList("testUser1-Data", "testUser2-Data"), config.getList("users.user.item"));
    }

    private String getTestInputData() {
        final DumperOptions yamlOptions = new DumperOptions();
        Yaml yaml = new Yaml(yamlOptions);

        yamlOptions.setIndent(YamlConfiguration.DEFAULT_IDENT);
        yamlOptions.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);

        return yaml.dump(this.getTestData());
    }

    private Map<Object, Object> getTestData() {
        final HashMap<Object, Object> result = new LinkedHashMap<>();

        result.put("integer", Integer.MIN_VALUE);
        result.put("string", "String Value");
        result.put("long", Long.MAX_VALUE);
        result.put("true-boolean", true);
        result.put("false-boolean", false);
        result.put("list", Arrays.asList(1, 2, 3, 4, 5));
        result.put("42", "The Answer");
        result.put("map", getSubMap());

        final Map<Object, Object> users = new LinkedHashMap<>();
        users.put("testUser1", Arrays.asList("testUser1-Data"));
        users.put("testUser2", Arrays.asList("testUser2-Data"));
        result.put("users", users);

        //result.put("tricky-list", Arrays.asList(Arrays.asList(Arrays.asList(leafMap.keySet()), leafMap.values())));

        return result;
    }

    private Map<Object, Object> getSubMap() {
        final Map<Object, Object> subMap = new LinkedHashMap<>();
        subMap.put("sub-string", "The String!");

        final Map<Object, Object> leafMap = new LinkedHashMap<>();
        leafMap.put("test", "value");
        leafMap.put("long", Long.MIN_VALUE);

        subMap.put("sub-list", Arrays.asList(Integer.MIN_VALUE, Arrays.asList("String1", "String2"), Boolean.TRUE, leafMap));
        return subMap;
    }
}
