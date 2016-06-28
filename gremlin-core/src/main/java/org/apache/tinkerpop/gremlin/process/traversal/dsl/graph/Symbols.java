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

package org.apache.tinkerpop.gremlin.process.traversal.dsl.graph;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class Symbols {

    private Symbols() {
        // static fields only
    }

    public static final String map = "map";
    public static final String flatMap = "flatMap";
    public static final String id = "id";
    public static final String label = "label";
    public static final String identity = "identity";
    public static final String constant = "constant";
    public static final String V = "V";
    public static final String to = "to";
    public static final String out = "out";
    public static final String in = "in";
    public static final String both = "both";
    public static final String toE = "toE";
    public static final String outE = "outE";
    public static final String inE = "inE";
    public static final String bothE = "bothE";
    public static final String toV = "toV";
    public static final String outV = "outV";
    public static final String inV = "inV";
    public static final String bothV = "bothV";
    public static final String otherV = "otherV";
    public static final String order = "order";
    public static final String properties = "properties";
    public static final String values = "values";
    public static final String propertyMap = "propertyMap";
    public static final String valueMap = "valueMap";
    public static final String select = "select";
    @Deprecated
    public static final String mapValues = "mapValues";
    @Deprecated
    public static final String mapKeys = "mapKeys";
    public static final String key = "key";
    public static final String value = "value";
    public static final String from = "from";
    public static final String path = "path";
    public static final String match = "match";
    public static final String sack = "sack";
    public static final String loops = "loops";
    public static final String project = "project";
    public static final String unfold = "unfold";
    public static final String fold = "fold";
    public static final String count = "count";
    public static final String sum = "sum";
}
