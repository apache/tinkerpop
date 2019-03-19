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
package org.apache.tinkerpop.machine.bytecode;

import org.apache.tinkerpop.machine.coefficient.Coefficient;
import org.apache.tinkerpop.machine.function.CFunction;
import org.apache.tinkerpop.machine.function.barrier.JoinBarrier;
import org.apache.tinkerpop.machine.function.barrier.StallBarrier;
import org.apache.tinkerpop.machine.function.branch.IfBranch;
import org.apache.tinkerpop.machine.function.branch.RepeatBranch;
import org.apache.tinkerpop.machine.function.branch.UnionBranch;
import org.apache.tinkerpop.machine.function.filter.FilterFilter;
import org.apache.tinkerpop.machine.function.filter.HasKeyFilter;
import org.apache.tinkerpop.machine.function.filter.HasKeyValueFilter;
import org.apache.tinkerpop.machine.function.filter.IdentityFilter;
import org.apache.tinkerpop.machine.function.filter.IsFilter;
import org.apache.tinkerpop.machine.function.flatmap.UnfoldFlatMap;
import org.apache.tinkerpop.machine.function.initial.InjectInitial;
import org.apache.tinkerpop.machine.function.map.ConstantMap;
import org.apache.tinkerpop.machine.function.map.IncrMap;
import org.apache.tinkerpop.machine.function.map.LoopsMap;
import org.apache.tinkerpop.machine.function.map.MapMap;
import org.apache.tinkerpop.machine.function.map.PathMap;
import org.apache.tinkerpop.machine.function.reduce.CountReduce;
import org.apache.tinkerpop.machine.function.reduce.GroupCountReduce;
import org.apache.tinkerpop.machine.function.reduce.SumReduce;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class CoreCompiler implements BytecodeCompiler {

    private static final CoreCompiler INSTANCE = new CoreCompiler();

    public static final CoreCompiler instance() {
        return INSTANCE;
    }

    private static final Map<String, FunctionType> OP_TYPES = new HashMap<>() {{
        put(Symbols.BARRIER, FunctionType.BARRIER);
        put(Symbols.IF, FunctionType.BRANCH);
        put(Symbols.CONSTANT, FunctionType.MAP);
        put(Symbols.COUNT, FunctionType.REDUCE);
        put(Symbols.FILTER, FunctionType.FILTER);
        put(Symbols.GROUP_COUNT, FunctionType.REDUCE);
        put(Symbols.HAS_KEY, FunctionType.FILTER);
        put(Symbols.HAS_KEY_VALUE, FunctionType.FILTER);
        put(Symbols.IDENTITY, FunctionType.FILTER);
        put(Symbols.INCR, FunctionType.MAP);
        put(Symbols.INJECT, FunctionType.INITIAL);
        put(Symbols.IS, FunctionType.FILTER);
        put(Symbols.JOIN, FunctionType.BARRIER);
        put(Symbols.LOOPS, FunctionType.MAP);
        put(Symbols.MAP, FunctionType.MAP);
        put(Symbols.PATH, FunctionType.MAP);
        put(Symbols.REPEAT, FunctionType.BRANCH);
        put(Symbols.SUM, FunctionType.REDUCE);
        put(Symbols.UNFOLD, FunctionType.FLATMAP);
        put(Symbols.UNION, FunctionType.BRANCH);
        put(Symbols.V, FunctionType.FLATMAP);
    }};

    @Override
    public <C> CFunction<C> compile(final Instruction<C> instruction) {
        final String op = instruction.op();
        final Coefficient<C> coefficient = instruction.coefficient();
        final Set<String> labels = instruction.labels();
        switch (op) {
            case Symbols.BARRIER:
                return new StallBarrier<>(coefficient, labels, 1000);
            case Symbols.IF:
                return new IfBranch<>(coefficient, labels,
                        Compilation.compileOne(instruction.args()[0]),
                        Compilation.compileOne(instruction.args()[1]),
                        Compilation.compileOrNull(2, instruction.args()));
            case Symbols.CONSTANT:
                return new ConstantMap<>(coefficient, labels, instruction.args()[0]);
            case Symbols.COUNT:
                return new CountReduce<>(coefficient, labels);
            case Symbols.FILTER:
                return new FilterFilter<>(coefficient, labels, Compilation.compileOne(instruction.args()[0]));
            case Symbols.GROUP_COUNT:
                return new GroupCountReduce<>(coefficient, labels, Compilation.compileOrNull(0, instruction.args()));
            case Symbols.HAS_KEY:
                return new HasKeyFilter<>(coefficient, labels, Pred.valueOf(instruction.args()[0]), Argument.create(instruction.args()[1]));
            case Symbols.HAS_KEY_VALUE:
                return new HasKeyValueFilter<>(coefficient, labels, Argument.create(instruction.args()[0]), Argument.create(instruction.args()[1]));
            case Symbols.IDENTITY:
                return new IdentityFilter<>(coefficient, labels);
            case Symbols.INJECT:
                return new InjectInitial<>(coefficient, labels, instruction.args());
            case Symbols.IS:
                return new IsFilter<>(coefficient, labels, Pred.valueOf(instruction.args()[0]), Argument.create(instruction.args()[1]));
            case Symbols.INCR:
                return new IncrMap<>(coefficient, labels);
            case Symbols.JOIN:
                return new JoinBarrier<>(coefficient, labels, (Symbols.Tokens) instruction.args()[0], Compilation.compileOne(instruction.args()[1]), Argument.create(instruction.args()[2]));
            case Symbols.LOOPS:
                return new LoopsMap<>(coefficient, labels);
            case Symbols.MAP:
                return new MapMap<>(coefficient, labels, Compilation.compileOne(instruction.args()[0]));
            case Symbols.PATH:
                return new PathMap<>(coefficient, labels, Compilation.compile(instruction.args()));
            case Symbols.REPEAT:
                return new RepeatBranch<>(coefficient, labels, Compilation.repeatCompile(instruction.args()));
            case Symbols.SUM:
                return new SumReduce<>(coefficient, labels);
            case Symbols.UNFOLD:
                return new UnfoldFlatMap<>(coefficient, labels);
            case Symbols.UNION:
                return new UnionBranch<>(coefficient, labels, Compilation.compile(instruction.args()));
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

        // INSTRUCTION OPS
        public static final String BARRIER = "barrier";
        // [if,[bc],[bc],?[bc]]
        public static final String IF = "if";
        // [constant, o]
        public static final String CONSTANT = "constant";
        // [count]
        public static final String COUNT = "count";
        public static final String EXPLAIN = "explain";
        // [filter, [bc]]
        public static final String FILTER = "filter";
        // [groupCount, ?[bc]]
        public static final String GROUP_COUNT = "groupCount";
        // [hasKey, pred, o | [bc]]
        public static final String HAS_KEY = "hasKey";
        public static final String HAS_KEY_VALUE = "hasKeyValue";
        // [identity]
        public static final String IDENTITY = "identity";
        // [incr]
        public static final String INCR = "incr";
        // [inject, o*]
        public static final String INJECT = "inject";
        // [is, pred, o | [bc]]
        public static final String IS = "is";
        public static final String JOIN = "join";
        // [loops]
        public static final String LOOPS = "loops";
        // [map, [bc]]
        public static final String MAP = "map";
        public static final String PATH = "path";
        // [repeat, (char, [bc]), ?(char, [bc]), ?(char, [bc])]
        public static final String REPEAT = "repeat";
        // [sum]
        public static final String SUM = "sum";
        // [unfold]
        public static final String UNFOLD = "unfold";
        // [union, [bc]*]
        public static final String UNION = "union";
        // [V]
        public static final String V = "V";

    }
}
