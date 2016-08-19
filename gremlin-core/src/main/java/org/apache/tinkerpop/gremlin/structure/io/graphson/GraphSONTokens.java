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
package org.apache.tinkerpop.gremlin.structure.io.graphson;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class GraphSONTokens {

    private GraphSONTokens() {}

    public static final String CLASS = "@class";
    public static final String VALUETYPE = "@type";
    public static final String VALUEPROP = "@value";
    public static final String ID = "id";
    public static final String TYPE = "type";
    public static final String VALUE = "value";
    public static final String PROPERTIES = "properties";
    public static final String KEY = "key";
    public static final String EDGE = "edge";
    public static final String EDGES = "edges";
    public static final String VERTEX = "vertex";
    public static final String VERTICES = "vertices";
    public static final String IN = "inV";
    public static final String OUT = "outV";
    public static final String IN_E = "inE";
    public static final String OUT_E = "outE";
    public static final String LABEL = "label";
    public static final String LABELS = "labels";
    public static final String OBJECTS = "objects";
    public static final String IN_LABEL = "inVLabel";
    public static final String OUT_LABEL = "outVLabel";
    public static final String GREMLIN_TYPE_DOMAIN = "gremlin";

    // TraversalExplanation Tokens
    public static final String ORIGINAL = "original";
    public static final String FINAL = "final";
    public static final String INTERMEDIATE = "intermediate";
    public static final String CATEGORY = "category";
    public static final String TRAVERSAL = "traversal";
    public static final String STRATEGY = "strategy";

    // TraversalMetrics Tokens
    public static final String METRICS = "metrics";
    public static final String DURATION = "dur";
    public static final String NAME = "name";
    public static final String COUNTS = "counts";
    public static final String ANNOTATIONS = "annotations";
}
