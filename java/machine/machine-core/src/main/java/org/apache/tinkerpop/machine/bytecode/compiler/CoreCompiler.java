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
import org.apache.tinkerpop.machine.function.barrier.StallBarrier;
import org.apache.tinkerpop.machine.function.branch.BranchBranch;
import org.apache.tinkerpop.machine.function.branch.RepeatBranch;
import org.apache.tinkerpop.machine.function.filter.FilterFilter;
import org.apache.tinkerpop.machine.function.flatmap.FlatMapFlatMap;
import org.apache.tinkerpop.machine.function.initial.InitialInitial;
import org.apache.tinkerpop.machine.function.map.MapMap;
import org.apache.tinkerpop.machine.function.map.PathMap;
import org.apache.tinkerpop.machine.function.reduce.GroupCountReduce;
import org.apache.tinkerpop.machine.function.reduce.ReduceReduce;

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
        put(Symbols.FILTER, FunctionType.FILTER);
        put(Symbols.FLATMAP, FunctionType.FLATMAP);
        put(Symbols.GROUP_COUNT, FunctionType.REDUCE);
        put(Symbols.INITIAL, FunctionType.INITIAL);
        put(Symbols.JOIN, FunctionType.BARRIER);
        put(Symbols.MAP, FunctionType.MAP);
        put(Symbols.PATH, FunctionType.MAP);
        put(Symbols.REDUCE, FunctionType.REDUCE);
        put(Symbols.REPEAT, FunctionType.BRANCH);
        put(Symbols.V, FunctionType.FLATMAP);
    }};

    @Override
    public <C> CFunction<C> compile(final Instruction<C> instruction) {
        switch (instruction.op()) {
            case Symbols.BARRIER:
                return StallBarrier.compile(instruction);
            case Symbols.BRANCH:
                return BranchBranch.compile(instruction);
            case Symbols.FILTER:
                return FilterFilter.compile(instruction);
            case Symbols.FLATMAP:
                return FlatMapFlatMap.compile(instruction);
            case Symbols.GROUP_COUNT:
                return GroupCountReduce.compile(instruction);
            case Symbols.INITIAL:
                return InitialInitial.compile(instruction);
            case Symbols.JOIN:
                return JoinBarrier.compile(instruction);
            case Symbols.MAP:
                return MapMap.compile(instruction);
            case Symbols.PATH:
                return PathMap.compile(instruction);
            case Symbols.REDUCE:
                return ReduceReduce.compile(instruction);
            case Symbols.REPEAT:
                return RepeatBranch.compile(instruction);
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
        public static final String WITH_MACHINE = "withMachine";
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
        public static final String EXPLAIN = "explain";
        // [filter, [bc]]
        public static final String FILTER = "filter";
        public static final String FLATMAP = "flatmap";
        // [groupCount, ?[bc]]
        public static final String GROUP_COUNT = "groupCount";
        // [incr]
        public static final String INITIAL = "initial";
        public static final String JOIN = "join";
        // [map, [bc]]
        public static final String MAP = "map";
        public static final String PATH = "path";
        public static final String REDUCE = "reduce";
        // [repeat, (char, [bc]), ?(char, [bc]), ?(char, [bc])]
        public static final String REPEAT = "repeat";
        // [V]
        public static final String V = "V";

    }
}
