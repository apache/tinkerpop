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
package org.apache.tinkerpop.gremlin.structure.io;

import io.netty.buffer.ByteBufAllocator;
import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryWriter;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONMapper;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONVersion;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONXModuleV3;
import org.apache.tinkerpop.gremlin.structure.io.graphson.TypeInfo;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerIoRegistryV3;
import org.apache.tinkerpop.gremlin.util.ser.AbstractGraphSONMessageSerializerV4;
import org.apache.tinkerpop.gremlin.util.ser.NettyBufferFactory;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintWriter;

/**
 * Generates the test files that are associated with the Model. This class uses relative paths so it is intended to be
 * run from the IDE and not from a JAR.
 */
public class GenerateGraphsonTestFiles {
    private static ObjectMapper typedGraphSonMapper = GraphSONMapper.build().
        addRegistry(TinkerIoRegistryV3.instance()).
        addCustomModule(GraphSONXModuleV3.build()).
        version(GraphSONVersion.V4_0).typeInfo(TypeInfo.PARTIAL_TYPES).create().createMapper();

//    private static ObjectMapper untypedGraphSonMapper = GraphSONMapper.build().
//            addRegistry(TinkerIoRegistryV3.instance()).
//            typeInfo(TypeInfo.NO_TYPES).
//            addCustomModule(GraphSONXModuleV3.build()).
//            version(GraphSONVersion.V4_0).create().createMapper();
    public static void main(String[] args) {
        String basePath = "./test-case-data/io/graphson";
        final File testDir = new File(basePath);

        if (testDir.exists() || testDir.mkdirs()) {
            Configuration conf = new BaseConfiguration();
            conf.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_DEFAULT_VERTEX_PROPERTY_CARDINALITY, VertexProperty.Cardinality.list.name());
            TinkerGraph graph = TinkerGraph.open(conf);
            TinkerFactory.generateTheCrew(graph);

            final Model model = Model.instance();

            PrintWriter printer = null;
            for (Model.Entry modelEntry : model.entries()) {
                try {
                    File fileToWriteTo = new File(basePath + "/" + modelEntry.getTitle().toLowerCase().replace(" ","") + "-" + "v4" + ".json");
                    System.out.println("Writing " + modelEntry.getTitle());
                    if (fileToWriteTo.exists()) fileToWriteTo.delete();
                    printer = new PrintWriter(fileToWriteTo);

                    String json = typedGraphSonMapper.writerWithDefaultPrettyPrinter().writeValueAsString(modelEntry.getObject());
                    printer.print(json);

                    printer.close();
                } catch (Exception ex) {
                    System.out.println("Exception occurred");
                    ex.printStackTrace();
                }
            }
        }
    }


}