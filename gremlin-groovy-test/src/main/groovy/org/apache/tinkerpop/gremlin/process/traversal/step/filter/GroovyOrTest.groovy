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
package org.apache.tinkerpop.gremlin.process.traversal.step.filter

import org.apache.tinkerpop.gremlin.process.UseEngine
import org.apache.tinkerpop.gremlin.process.computer.ComputerTestHelper
import org.apache.tinkerpop.gremlin.process.traversal.Traversal
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine
import org.apache.tinkerpop.gremlin.structure.T
import org.apache.tinkerpop.gremlin.structure.Vertex

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.has
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.outE
import static org.apache.tinkerpop.gremlin.structure.P.gt
import static org.apache.tinkerpop.gremlin.structure.P.gte;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyOrTest {

    @UseEngine(TraversalEngine.Type.STANDARD)
    public static class StandardTraversals extends OrTest {

        @Override
        public Traversal<Vertex, String> get_g_V_orXhasXage_gt_27X__outE_count_gte_2X_name() {
            g.V.or(has('age', gt(27)), outE().count.is(gte(2l))).name
        }

        @Override
        public Traversal<Vertex, String> get_g_V_orXoutEXknowsX__hasXlabel_softwareX_or_hasXage_gte_35XX_name() {
            g.V.or(outE('knows'), has(T.label, 'software') | has('age', gte(35))).name
        }
    }

    @UseEngine(TraversalEngine.Type.COMPUTER)
    public static class ComputerTraversals extends OrTest {

        @Override
        public Traversal<Vertex, String> get_g_V_orXhasXage_gt_27X__outE_count_gte_2X_name() {
            ComputerTestHelper.compute("g.V.or(has('age', gt(27)), outE().count.is(gte(2l))).name", g)
        }

        @Override
        public Traversal<Vertex, String> get_g_V_orXoutEXknowsX__hasXlabel_softwareX_or_hasXage_gte_35XX_name() {
            ComputerTestHelper.compute("g.V.or(outE('knows'), has(T.label, 'software') | has('age', gte(35))).name", g)
        }
    }
}
