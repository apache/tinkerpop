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

package org.apache.tinkerpop.gremlin.process.traversal.util;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class DefaultTraversalMetricsTest {

    @Test
    public void shouldPrintIndentationsCorrectly() {
        List<MutableMetrics> metrics = new ArrayList<>();
        MutableMetrics rootMetrics = new MutableMetrics("1", "GraphStep");
        metrics.add(rootMetrics);

        MutableMetrics queryMetrics = new MutableMetrics("1.1", "GraphQuery");
        queryMetrics.setAnnotation("condition", "name = Bob");
        rootMetrics.addNested(queryMetrics);

        MutableMetrics childMetrics = new MutableMetrics("1.1.1", "AND-Query");
        childMetrics.setAnnotation("index", "gIndex");
        queryMetrics.addNested(childMetrics);

        MutableMetrics backendMetrics = new MutableMetrics("1.1.1.1", "backend-query");
        backendMetrics.setAnnotation("query", "gIndex:slice-query");
        childMetrics.addNested(backendMetrics);

        DefaultTraversalMetrics profile = new DefaultTraversalMetrics(100, metrics);
        final String expectedOutput = "Traversal Metrics\n" +
                "Step                                                               Count  Traversers       Time (ms)    % Dur\n" +
                "=============================================================================================================\n" +
                "GraphStep                                                                                      0.000\n" +
                "  GraphQuery                                                                                   0.000\n" +
                "    \\_condition=name = Bob\n" +
                "    AND-Query                                                                                  0.000\n" +
                "      \\_index=gIndex\n" +
                "      backend-query                                                                            0.000\n" +
                "        \\_query=gIndex:slice-query\n" +
                "                                            >TOTAL                     -           -           0.000        -";
        assertEquals(expectedOutput, profile.toString());
    }
}
