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
package org.apache.tinkerpop.gremlin.structure.io.graphson;

import org.apache.commons.io.IOUtils;
import org.apache.tinkerpop.gremlin.structure.io.AbstractTypedCompatibilityTest;
import org.apache.tinkerpop.gremlin.structure.io.Model;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerIoRegistryV2;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerIoRegistryV3;
import org.apache.tinkerpop.gremlin.util.TestSupport;
import org.apache.tinkerpop.gremlin.util.ser.AbstractGraphSONMessageSerializerV2;
import org.apache.tinkerpop.gremlin.util.ser.GraphSONMessageSerializerV3;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(Parameterized.class)
public class GraphSONTypedCompatibilityTest extends AbstractTypedCompatibilityTest {

    private static ObjectMapper mapperV2 = GraphSONMapper.build().
            addRegistry(TinkerIoRegistryV2.instance()).
            typeInfo(TypeInfo.PARTIAL_TYPES).
            addCustomModule(GraphSONXModuleV2.build()).
            addCustomModule(new AbstractGraphSONMessageSerializerV2.GremlinServerModule()).
            version(GraphSONVersion.V2_0).create().createMapper();

    private static ObjectMapper mapperV3 = GraphSONMapper.build().
            addRegistry(TinkerIoRegistryV3.instance()).
            addCustomModule(GraphSONXModuleV3.build()).
            addCustomModule(new GraphSONMessageSerializerV3.GremlinServerModule()).
            version(GraphSONVersion.V3_0).create().createMapper();

    private static final String testCaseDataPath = root.getPath() + File.separator + "test-case-data" + File.separator
        + "io" + File.separator + "graphson";

    static {
        resetDirectory(testCaseDataPath);
    }

    @Parameterized.Parameters(name = "expect({0})")
    public static Iterable<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {"v2", mapperV2 },
                {"v3", mapperV3 },
        });
    }

    @Parameterized.Parameter(value = 0)
    public String compatibility;

    @Parameterized.Parameter(value = 1)
    public ObjectMapper mapper;

    @Override
    protected String getCompatibility() {
        return compatibility;
    }

    @Override
    protected byte[] readFromResource(final String resource) throws IOException {
        final String testResource = resource + "-" + compatibility + ".json";
        return IOUtils.toByteArray(getClass().getResourceAsStream(testResource));
    }

    @Override
    public <T> T read(final byte[] bytes, final Class<T> clazz) throws Exception {
        return mapper.readValue(bytes, clazz);
    }

    @Override
    public byte[] write(final Object o, final Class<?> clazz, final String entryName) throws Exception  {
        final byte[] bytes =  mapper.writeValueAsBytes(o);

        // write out files for debugging purposes
        final File f = new File(testCaseDataPath + File.separator + entryName + "-" + getCompatibility() + ".json");
        if (f.exists()) f.delete();
        try (FileOutputStream fileOuputStream = new FileOutputStream(f)) {
            fileOuputStream.write(bytes);
        }

        return bytes;
    }
}
