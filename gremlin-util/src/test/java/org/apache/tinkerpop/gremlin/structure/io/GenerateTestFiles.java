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
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryWriter;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.util.ser.NettyBufferFactory;

import java.io.File;
import java.io.FileOutputStream;

/**
 * Generates the test files that are associated with the Model. This class uses relative paths so it is intended to be
 * run from the IDE and not from a JAR.
 */
public class GenerateTestFiles {
    private static GraphBinaryWriter graphBinaryWriter = new GraphBinaryWriter();
    public static void main(String[] args) {
        String basePath = "./test-case-data/io/graphbinary";
        final File testDir = new File(basePath);

        if (testDir.exists() || testDir.mkdirs()) {
            Configuration conf = new BaseConfiguration();
            conf.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_DEFAULT_VERTEX_PROPERTY_CARDINALITY, VertexProperty.Cardinality.list.name());
            TinkerGraph graph = TinkerGraph.open(conf);
            TinkerFactory.generateTheCrew(graph);

            final Model model = Model.instance();

            ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;
            NettyBufferFactory factory = new NettyBufferFactory();
            FileOutputStream filestream = null;
            Buffer buffer = null;
            for (Model.Entry modelEntry : model.entries()) {
                try {
                    File fileToWriteTo = new File(basePath + "/" + modelEntry.getTitle().toLowerCase().replace(" ","") + "-" + "v4" + ".gbin");
                    System.out.println("Writing " + modelEntry.getTitle());
                    if (fileToWriteTo.exists()) fileToWriteTo.delete();
                    filestream = new FileOutputStream(fileToWriteTo);

                    buffer = factory.create(allocator.buffer());
                    graphBinaryWriter.write(modelEntry.getObject(), buffer);
                    buffer.readerIndex(0);
                    buffer.readBytes(filestream, buffer.readableBytes());
                    filestream.close();
                    buffer.release();
                } catch (Exception ex) {
                    System.out.println("Exception occurred");
                    ex.printStackTrace();
                }
            }
        }
    }


}