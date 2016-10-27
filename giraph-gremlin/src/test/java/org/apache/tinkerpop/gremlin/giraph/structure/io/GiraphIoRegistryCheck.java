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

package org.apache.tinkerpop.gremlin.giraph.structure.io;

import org.apache.tinkerpop.gremlin.giraph.process.computer.GiraphGraphComputer;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopGraph;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.AbstractIoRegistryCheck;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.HadoopPools;
import org.apache.tinkerpop.gremlin.structure.io.gryo.kryoshim.KryoShimServiceLoader;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GiraphIoRegistryCheck extends AbstractIoRegistryCheck {

    @Before
    public void setup() throws Exception {
        super.setup();
        KryoShimServiceLoader.close();
        HadoopPools.close();
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        KryoShimServiceLoader.close();
        HadoopPools.close();
    }

    @Test
    public void shouldSupportGryoIoRegistry() throws Exception {
        super.checkGryoIoRegistryCompliance((HadoopGraph) graph, GiraphGraphComputer.class);
    }

    @Test
    public void shouldSupportGraphSONIoRegistry() throws Exception {
        super.checkGraphSONIoRegistryCompliance((HadoopGraph) graph, GiraphGraphComputer.class);
    }
}
