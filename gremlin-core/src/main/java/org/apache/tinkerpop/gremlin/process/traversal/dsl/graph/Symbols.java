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
    public static final String E = "E";
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
    public static final String path = "path";
    public static final String match = "match";
    public static final String sack = "sack";
    public static final String loops = "loops";
    public static final String project = "project";
    public static final String unfold = "unfold";
    public static final String fold = "fold";
    public static final String count = "count";
    public static final String sum = "sum";
    public static final String max = "max";
    public static final String min = "min";
    public static final String mean = "mean";
    public static final String group = "group";
    @Deprecated
    public static final String groupV3d0 = "groupV3d0";
    public static final String groupCount = "groupCount";
    public static final String tree = "tree";
    public static final String addV = "addV";
    public static final String addE = "addE";
    public static final String from = "from";
    public static final String filter = "filter";
    public static final String or = "or";
    public static final String and = "and";
    public static final String inject = "inject";
    public static final String dedup = "dedup";
    public static final String where = "where";
    public static final String has = "has";
    public static final String hasNot = "hasNot";
    public static final String hasLabel = "hasLabel";
    public static final String hasId = "hasId";
    public static final String hasKey = "hasKey";
    public static final String hasValue = "hasValue";
    public static final String is = "is";
    public static final String not = "not";
    public static final String range = "range";
    public static final String limit = "limit";
    public static final String tail = "tail";
    public static final String coin = "coin";

    public static final String timeLimit = "timeLimit";
    public static final String simplePath = "simplePath";
    public static final String cyclicPath = "cyclicPath";
    public static final String sample = "sample";

    public static final String drop = "drop";

    public static final String sideEffect = "sideEffect";
    public static final String cap = "cap";
    public static final String store = "store";
    public static final String aggregate = "aggregate";
    public static final String subgraph = "subgraph";
    public static final String profile = "profile";
    public static final String barrier = "barrier";
    public static final String local = "local";
    public static final String emit = "emit";
    public static final String repeat = "repeat";
    public static final String until = "until";
    public static final String branch = "branch";
    public static final String union = "union";
    public static final String coalesce = "coalesce";
    public static final String choose = "choose";
    public static final String optional = "optional";

    public static final String pageRank = "pageRank";
    public static final String peerPressure = "peerPressure";
    public static final String program = "program";

    public static final String by = "by";
    public static final String times = "times";
    public static final String as = "as";
    public static final String option = "option";

    public static final String withPath = "withPath";
    public static final String withBulk = "withBulk";
}
