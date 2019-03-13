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
package org.apache.tinkerpop.language;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class Symbols {

    public static enum Type {
        INITIAL, MAP, FLATMAP, FILTER, REDUCE, BRANCH
    }

    // SOURCE OPS
    public static final String WITH_COEFFICIENT = "withCoefficient";
    public static final String WITH_PROCESSOR = "withProcessor";
    public static final String WITH_STRATEGY = "withStrategy";


    // INSTRUCTION OPS
    public static final String CHOOSE_IF_THEN_ELSE = "chooseIfThenElse";
    public static final String COUNT = "count";
    public static final String FILTER = "filter";
    public static final String GROUP_COUNT = "groupCount";
    public static final String HAS_KEY = "hasKey";
    public static final String HAS_KEY_VALUE = "hasKeyValue";
    public static final String IDENTITY = "identity";
    public static final String INCR = "incr";
    public static final String INJECT = "inject";
    public static final String IS = "is";
    public static final String MAP = "map";
    public static final String PATH = "path";
    public static final String REPEAT = "repeat";
    public static final String SUM = "sum";
    public static final String UNFOLD = "unfold";
    public static final String UNION = "union";

    public Type getOpType(final String op) {
        switch (op) {
            case COUNT:
                return Type.REDUCE;
            case FILTER:
                return Type.FILTER;
            case GROUP_COUNT:
                return Type.REDUCE;
            case HAS_KEY:
                return Type.FILTER;
            case HAS_KEY_VALUE:
                return Type.FILTER;
            case IDENTITY:
                return Type.FILTER;
            case INCR:
                return Type.MAP;
            case INJECT:
                return Type.INITIAL;
            case IS:
                return Type.FILTER;
            case MAP:
                return Type.MAP;
            case PATH:
                return Type.MAP;
            case REPEAT:
                return Type.BRANCH;
            case SUM:
                return Type.REDUCE;
            case UNFOLD:
                return Type.FLATMAP;
            case UNION:
                return Type.BRANCH;
            default:
                throw new IllegalArgumentException("The following op is unknown: " + op);
        }
    }
}
