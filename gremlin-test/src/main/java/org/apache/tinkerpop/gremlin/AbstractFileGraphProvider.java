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
package org.apache.tinkerpop.gremlin;

import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.features.TestFiles;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.Random;

/**
 * A base {@link GraphProvider} that is typically for use with Hadoop-based graphs as it enables access to the various
 * resource data files that are used in the tests.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class AbstractFileGraphProvider extends AbstractGraphProvider {

    protected static final Random RANDOM = TestHelper.RANDOM;

    protected boolean graphSONInput = false;

    @Override
    public void clear(final Graph graph, final Configuration configuration) throws Exception {
        if (graph != null)
            graph.close();
    }

    protected String getInputLocation(final Graph g, final LoadGraphWith.GraphData graphData) {
        return TestFiles.getInputLocation(graphData, graphSONInput);
    }
}
