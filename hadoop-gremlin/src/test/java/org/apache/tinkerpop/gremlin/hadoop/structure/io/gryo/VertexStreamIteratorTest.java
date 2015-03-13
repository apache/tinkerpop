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
package org.apache.tinkerpop.gremlin.hadoop.structure.io.gryo;

import org.apache.tinkerpop.gremlin.hadoop.process.computer.giraph.GiraphComputeVertex;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.VertexWritable;
import org.junit.Ignore;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class VertexStreamIteratorTest {

    @Test
    @Ignore
    public void testStuff() throws Exception {
        FileInputStream input = new FileInputStream("/tmp/100K.gio");
        VertexStreamIterator iterator = new VertexStreamIterator(input, 15542136);
        int counter = 0;
        long time = System.currentTimeMillis();
        while (iterator.hasNext()) {
            if (++counter % 1000 == 0) {
                System.out.println("Read vertices: " + counter + "[ms:" + (System.currentTimeMillis() - time) + "]");
                System.out.println(iterator.getProgress() + " -- progress");
                time = System.currentTimeMillis();
            }
            final ByteArrayOutputStream out = new ByteArrayOutputStream();
            //System.out.println("HERE:" +IteratorUtils.count(vertex.get().edges(Direction.BOTH)));
            final GiraphComputeVertex vertex = new GiraphComputeVertex(iterator.next());
            //System.out.println(vertex + "!!!!");
            final DataOutputStream output = new DataOutputStream(out);
            vertex.getValue().write(output);
            output.flush();
            //System.out.println("!!!" + out.size());
            final VertexWritable v = new VertexWritable();
            final ByteArrayInputStream inputStream = new ByteArrayInputStream(out.toByteArray());
            v.readFields(new DataInputStream(inputStream));
        }
    }
}
