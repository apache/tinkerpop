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
package org.apache.tinkerpop.gremlin.spark.util;

import org.apache.tinkerpop.gremlin.groovy.util.MetaRegistryUtil;
import org.apache.tinkerpop.gremlin.hadoop.HadoopGraphProvider;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.*;

import java.util.HashSet;
import java.util.Set;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class SugarTestHelper {

    public static final Set<Class> CORE_IMPLEMENTATIONS = new HashSet<Class>() {{
        add(__.class);
        add(DefaultGraphTraversal.class);
        add(GraphTraversalSource.class);
        add(B_O_S_SE_SL_Traverser.class);
        add(B_LP_O_P_S_SE_SL_Traverser.class);
        add(B_LP_O_S_SE_SL_Traverser.class);
        add(B_O_Traverser.class);
        add(O_Traverser.class);
    }};

    /**
     * Clear the metaclass registry to "turn-off" sugar.
     */
    public static void clearRegistry() {
        final Set<Class> implementationsToClear = new HashSet<>(CORE_IMPLEMENTATIONS);
        implementationsToClear.addAll(HadoopGraphProvider.IMPLEMENTATION);

        MetaRegistryUtil.clearRegistry(implementationsToClear);
    }
}
