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
package org.apache.tinkerpop.machine.bytecode.compiler;

import org.apache.tinkerpop.machine.bytecode.Instruction;
import org.apache.tinkerpop.machine.function.CFunction;
import org.apache.tinkerpop.machine.function.barrier.JoinBarrier;
import org.apache.tinkerpop.machine.function.barrier.OrderBarrier;
import org.apache.tinkerpop.machine.function.barrier.StallBarrier;
import org.apache.tinkerpop.machine.function.branch.BranchBranch;
import org.apache.tinkerpop.machine.function.branch.RepeatBranch;
import org.apache.tinkerpop.machine.function.filter.FilterFilter;
import org.apache.tinkerpop.machine.function.filter.HasKeyFilter;
import org.apache.tinkerpop.machine.function.filter.HasKeyValueFilter;
import org.apache.tinkerpop.machine.function.filter.IdentityFilter;
import org.apache.tinkerpop.machine.function.filter.IsFilter;
import org.apache.tinkerpop.machine.function.flatmap.DbFlatMap;
import org.apache.tinkerpop.machine.function.flatmap.EntriesFlatMap;
import org.apache.tinkerpop.machine.function.flatmap.FlatMapFlatMap;
import org.apache.tinkerpop.machine.function.flatmap.InjectFlatMap;
import org.apache.tinkerpop.machine.function.flatmap.UnfoldFlatMap;
import org.apache.tinkerpop.machine.function.flatmap.ValuesFlatMap;
import org.apache.tinkerpop.machine.function.map.ConstantMap;
import org.apache.tinkerpop.machine.function.map.IncrMap;
import org.apache.tinkerpop.machine.function.map.LoopsMap;
import org.apache.tinkerpop.machine.function.map.MapMap;
import org.apache.tinkerpop.machine.function.map.PathMap;
import org.apache.tinkerpop.machine.function.map.ValueMap;
import org.apache.tinkerpop.machine.function.reduce.CountReduce;
import org.apache.tinkerpop.machine.function.reduce.GroupCountReduce;
import org.apache.tinkerpop.machine.function.reduce.ReduceReduce;
import org.apache.tinkerpop.machine.function.reduce.SumReduce;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class CoreCompiler implements BytecodeCompiler {

    private static final CoreCompiler INSTANCE = new CoreCompiler();

    private CoreCompiler() {
        // static instance
    }

    public static CoreCompiler instance() {
        return INSTANCE;
    }

    private static final Map<String, FunctionType> OP_TYPES = new HashMap<>() {{
        put(Symbols.BARRIER, FunctionType.BARRIER);
        put(Symbols.BRANCH, FunctionType.BRANCH);
        put(Symbols.CONSTANT, FunctionType.MAP);
        put(Symbols.COUNT, FunctionType.REDUCE);
        put(Symbols.ENTRIES, FunctionType.FLATMAP);
        put(Symbols.FILTER, FunctionType.FILTER);
        put(Symbols.FLATMAP, FunctionType.FLATMAP);
        put(Symbols.GROUP_COUNT, FunctionType.REDUCE);
        put(Symbols.HAS_KEY, FunctionType.FILTER);
        put(Symbols.HAS_KEY_VALUE, FunctionType.FILTER);
        put(Symbols.IDENTITY, FunctionType.FILTER);
        put(Symbols.INCR, FunctionType.MAP);
        put(Symbols.INJECT, FunctionType.FLATMAP);
        put(Symbols.IS, FunctionType.FILTER);
        put(Symbols.JOIN, FunctionType.BARRIER);
        put(Symbols.LOOPS, FunctionType.MAP);
        put(Symbols.MAP, FunctionType.MAP);
        put(Symbols.ORDER, FunctionType.BARRIER);
        put(Symbols.PATH, FunctionType.MAP);
        put(Symbols.REDUCE, FunctionType.REDUCE);
        put(Symbols.REPEAT, FunctionType.BRANCH);
        put(Symbols.SUM, FunctionType.REDUCE);
        put(Symbols.UNFOLD, FunctionType.FLATMAP);
        put(Symbols.VALUE, FunctionType.MAP);
        put(Symbols.VALUES, FunctionType.FLATMAP);

        //
        put(Symbols.DB, FunctionType.FLATMAP);
        put(Symbols.V, FunctionType.FLATMAP);
    }};

    @Override
    public <C> CFunction<C> compile(final Instruction<C> instruction) {
        switch (instruction.op()) {
            case Symbols.BARRIER:
                return StallBarrier.compile(instruction);
            case Symbols.BRANCH:
                return BranchBranch.compile(instruction);
            case Symbols.CONSTANT:
                return ConstantMap.compile(instruction);
            case Symbols.COUNT:
                return CountReduce.compile(instruction);
            case Symbols.ENTRIES:
                return EntriesFlatMap.compile(instruction);
            case Symbols.FILTER:
                return FilterFilter.compile(instruction);
            case Symbols.FLATMAP:
                return FlatMapFlatMap.compile(instruction);
            case Symbols.GROUP_COUNT:
                return GroupCountReduce.compile(instruction);
            case Symbols.HAS_KEY:
                return HasKeyFilter.compile(instruction);
            case Symbols.HAS_KEY_VALUE:
                return HasKeyValueFilter.compile(instruction);
            case Symbols.IDENTITY:
                return IdentityFilter.compile(instruction);
            case Symbols.INCR:
                return IncrMap.compile(instruction);
            case Symbols.INJECT:
                return InjectFlatMap.compile(instruction);
            case Symbols.IS:
                return IsFilter.compile(instruction);
            case Symbols.JOIN:
                return JoinBarrier.compile(instruction);
            case Symbols.LOOPS:
                return LoopsMap.compile(instruction);
            case Symbols.MAP:
                return MapMap.compile(instruction);
            case Symbols.ORDER:
                return OrderBarrier.compile(instruction);
            case Symbols.PATH:
                return PathMap.compile(instruction);
            case Symbols.REDUCE:
                return ReduceReduce.compile(instruction);
            case Symbols.REPEAT:
                return RepeatBranch.compile(instruction);
            case Symbols.SUM:
                return SumReduce.compile(instruction);
            case Symbols.UNFOLD:
                return UnfoldFlatMap.compile(instruction);
            case Symbols.VALUE:
                return ValueMap.compile(instruction);
            case Symbols.VALUES:
                return ValuesFlatMap.compile(instruction);
            case Symbols.DB:
                return DbFlatMap.compile(instruction);
            default:
                return null;
        }
    }

    @Override
    public FunctionType getFunctionType(final String op) {
        return OP_TYPES.get(op);
    }

    public static final class Symbols {

        private Symbols() {
            // static instance
        }

        public static enum Tokens {
            inner, left, right, full
        }

        // SOURCE OPS
        public static final String WITH_COEFFICIENT = "withCoefficient";
        public static final String WITH_PROCESSOR = "withProcessor";
        public static final String WITH_STRUCTURE = "withStructure";
        public static final String WITH_STRATEGY = "withStrategy";


        // ARGUMENT TOKENS
        public static final String EQ = "eq";
        public static final String NEQ = "neq";
        public static final String LT = "lt";
        public static final String GT = "gt";
        public static final String LTE = "lte";
        public static final String GTE = "gte";
        public static final String REGEX = "regex";

        // BRANCH TOKENS
        public static final String DEFAULT = "default";

        // INSTRUCTION OPS
        public static final String BARRIER = "barrier";
        public static final String BRANCH = "branch";
        public static final String CONSTANT = "constant";
        public static final String COUNT = "count";
        public static final String ENTRIES = "entries";
        public static final String EXPLAIN = "explain";
        public static final String FILTER = "filter";
        public static final String FLATMAP = "flatmap";
        public static final String GROUP_COUNT = "groupCount";
        public static final String HAS_KEY = "hasKey";
        public static final String HAS_KEY_VALUE = "hasKeyValue";
        public static final String IDENTITY = "identity";
        public static final String INCR = "incr";
        public static final String INJECT = "initial";
        public static final String IS = "is";
        public static final String JOIN = "join";
        public static final String LOOPS = "loops";
        public static final String MAP = "map";
        public static final String ORDER = "order";
        public static final String PATH = "path";
        public static final String REDUCE = "reduce";
        public static final String REPEAT = "repeat";
        public static final String SUM = "sum";
        public static final String UNFOLD = "unfold";
        public static final String V = "V";
        public static final String VALUE = "value";
        public static final String VALUES = "values";

        //
        public static final String DB = "db";
    }
}