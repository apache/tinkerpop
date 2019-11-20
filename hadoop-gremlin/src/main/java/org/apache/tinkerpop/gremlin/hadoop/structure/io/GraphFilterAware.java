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

package org.apache.tinkerpop.gremlin.hadoop.structure.io;

import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.hadoop.Constants;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.GraphFilter;
import org.apache.tinkerpop.gremlin.process.computer.util.VertexProgramHelper;

/**
 * An input graph class is {@code GraphFilterAware} if it can filter out vertices and edges as its loading the graph
 * from the source data. Any input class that is {@code GraphFilterAware} must be able to fully handle both vertex and
 * edge filtering. It is assumed that if the input class is {@code GraphFilterAware}, then the respective
 * {@link GraphComputer} will not perform any filtering on the loaded graph.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface GraphFilterAware {

    public void setGraphFilter(final GraphFilter graphFilter);

    public static void storeGraphFilter(final Configuration apacheConfiguration, final org.apache.hadoop.conf.Configuration hadoopConfiguration, final GraphFilter graphFilter) {
        if (graphFilter.hasFilter()) {
            VertexProgramHelper.serialize(graphFilter, apacheConfiguration, Constants.GREMLIN_HADOOP_GRAPH_FILTER);
            hadoopConfiguration.set(Constants.GREMLIN_HADOOP_GRAPH_FILTER, apacheConfiguration.getString(Constants.GREMLIN_HADOOP_GRAPH_FILTER));
        }
    }
}
