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
package org.apache.tinkerpop.gremlin.structure.io.graphml;

/**
 * A collection of tokens used for GraphML related data.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
final class GraphMLTokens {
    private GraphMLTokens() {}

    public static final String XML_SCHEMA_NAMESPACE_TAG = "xsi";
    public static final String DEFAULT_GRAPHML_SCHEMA_LOCATION = "http://graphml.graphdrawing.org/xmlns/1.1/graphml.xsd";
    public static final String XML_SCHEMA_LOCATION_ATTRIBUTE = "schemaLocation";
    public static final String GRAPHML = "graphml";
    public static final String XMLNS = "xmlns";
    public static final String GRAPHML_XMLNS = "http://graphml.graphdrawing.org/xmlns";
    public static final String G = "G";
    public static final String EDGEDEFAULT = "edgedefault";
    public static final String DIRECTED = "directed";
    public static final String KEY = "key";
    public static final String FOR = "for";
    public static final String ID = "id";
    public static final String ATTR_NAME = "attr.name";
    public static final String ATTR_TYPE = "attr.type";
    public static final String GRAPH = "graph";
    public static final String NODE = "node";
    public static final String EDGE = "edge";
    public static final String DEFAULT = "default";
    public static final String SOURCE = "source";
    public static final String TARGET = "target";
    public static final String DATA = "data";
    public static final String LABEL_E = "labelE";
    public static final String LABEL_V = "labelV";
    public static final String STRING = "string";
    public static final String FLOAT = "float";
    public static final String DOUBLE = "double";
    public static final String LONG = "long";
    public static final String BOOLEAN = "boolean";
    public static final String INT = "int";
    public static final String VERTEX_SUFFIX = "V";
    public static final String EDGE_SUFFIX = "E";
}
