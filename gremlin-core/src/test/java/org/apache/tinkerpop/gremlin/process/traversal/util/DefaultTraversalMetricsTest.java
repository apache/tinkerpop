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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class DefaultTraversalMetricsTest {
static final Logger logger = LoggerFactory.getLogger(DefaultTraversalMetricsTest.class);
    @Test
    public void shouldPrintIndentationsCorrectly() {
        final List<MutableMetrics> metrics = new ArrayList<>();
        final MutableMetrics rootMetrics = new MutableMetrics("1", "GraphStep");
        metrics.add(rootMetrics);

        final MutableMetrics queryMetrics = new MutableMetrics("1.1", "GraphQuery");
        queryMetrics.setAnnotation("condition", "name = Bob");
        rootMetrics.addNested(queryMetrics);

        final MutableMetrics childMetrics = new MutableMetrics("1.1.1", "AND-Query");
        childMetrics.setAnnotation("index", "gIndex");
        childMetrics.setAnnotation("query-hint", "ZSORTED");
        queryMetrics.addNested(childMetrics);

        final MutableMetrics backendMetrics = new MutableMetrics("1.1.1.1", "backend-query");
        backendMetrics.setAnnotation("query", "gIndex:slice-query");
        childMetrics.addNested(backendMetrics);

        final DefaultTraversalMetrics profile = new DefaultTraversalMetrics(100, metrics);
        final String LS = System.lineSeparator();
        final String expectedOutput = "Traversal Metrics\n" +
                "Step                                                               Count  Traversers       Time (ms)    % Dur\n" +
                "=============================================================================================================" + LS +
                "GraphStep                                                                                      0.000" + LS +
                "  GraphQuery                                                                                   0.000" + LS +
                "    \\_condition=name = Bob" + LS +
                "    AND-Query                                                                                  0.000" + LS +
                "      \\_index=gIndex" + LS +
                "      \\_query-hint=ZSORTED" + LS +
                "      backend-query                                                                            0.000" + LS +
                "        \\_query=gIndex:slice-query" + LS +
                "                                            >TOTAL                     -           -           0.000        -";
        assertEquals("traversal metrics", expectedOutput, profile.toString());
    }
}
