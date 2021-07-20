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
package org.apache.tinkerpop.gremlin.language.grammar;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;

/**
 * Test for parse Graph traversal enums
 */
@RunWith(Enclosed.class)
public class TraversalEnumParserTest {

    @RunWith(Parameterized.class)
    public static class EnumTest {

        @Parameterized.Parameter(value = 0)
        public String className;

        @Parameterized.Parameter(value = 1)
        public String parseMethodName;

        @Parameterized.Parameters()
        public static Iterable<Object[]> generateTestParameters() {
            return Arrays.asList(new Object[][]{
                    {"org.apache.tinkerpop.gremlin.process.traversal.Scope", "traversalScope"},
                    {"org.apache.tinkerpop.gremlin.process.traversal.Order", "traversalOrder"},
                    {"org.apache.tinkerpop.gremlin.process.traversal.Pop", "traversalPop"},
                    {"org.apache.tinkerpop.gremlin.process.traversal.SackFunctions$Barrier", "traversalSackMethod"},
                    {"org.apache.tinkerpop.gremlin.process.traversal.Operator", "traversalOperator"},
                    {"org.apache.tinkerpop.gremlin.structure.T", "traversalToken"},
                    {"org.apache.tinkerpop.gremlin.structure.Column", "traversalColumn"},
                    {"org.apache.tinkerpop.gremlin.structure.Direction", "traversalDirection"},
                    {"org.apache.tinkerpop.gremlin.structure.VertexProperty$Cardinality", "traversalCardinality"},
                    {"org.apache.tinkerpop.gremlin.process.traversal.step.TraversalOptionParent$Pick", "traversalOptionParent"},
            });
        }

        @Test
        public void testAllEnumTypes() throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {
            final Class<? extends Enum> enumType = (Class<? extends Enum>) (Class.forName(className));
            for (Enum enumConstant : enumType.getEnumConstants()) {
                final GremlinLexer lexer = new GremlinLexer(CharStreams.fromString(enumConstant.name()));
                final GremlinParser parser = new GremlinParser(new CommonTokenStream(lexer));

                final Method method = parser.getClass().getMethod(parseMethodName);
                final ParseTree ctx = (ParseTree) (method.invoke(parser));
                assertEquals(enumConstant, TraversalEnumParser.parseTraversalEnumFromContext(enumType, ctx));
            }
        }
    }

    @RunWith(Parameterized.class)
    public static class QualifiedEnumTest {

        @Parameterized.Parameter(value = 0)
        public String className;

        @Parameterized.Parameter(value = 1)
        public String parseMethodName;

        @Parameterized.Parameters()
        public static Iterable<Object[]> generateTestParameters() {
            return Arrays.asList(new Object[][]{
                    {"org.apache.tinkerpop.gremlin.process.traversal.Scope", "traversalScope"},
                    {"org.apache.tinkerpop.gremlin.process.traversal.Order", "traversalOrder"},
                    {"org.apache.tinkerpop.gremlin.structure.T", "traversalToken"}
            });
        }

        @Test
        public void testAllQualifiedEnumTypes() throws
        ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {
            Class<? extends Enum> enumType = (Class<? extends Enum>) (Class.forName(className));
            for (Enum enumConstant : enumType.getEnumConstants()) {
                String testExpr = enumType.getSimpleName() + "." + enumConstant.name();
                GremlinLexer lexer = new GremlinLexer(CharStreams.fromString(testExpr));
                GremlinParser parser = new GremlinParser(new CommonTokenStream(lexer));

                Method method = parser.getClass().getMethod(parseMethodName);
                ParseTree ctx = (ParseTree) (method.invoke(parser));
                assertEquals(enumConstant, TraversalEnumParser.parseTraversalEnumFromContext(enumType, ctx));
            }
        }
    }
}
