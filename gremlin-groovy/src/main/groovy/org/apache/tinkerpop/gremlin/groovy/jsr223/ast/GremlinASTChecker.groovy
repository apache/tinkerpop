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
package org.apache.tinkerpop.gremlin.groovy.jsr223.ast

import org.codehaus.groovy.ast.CodeVisitorSupport
import org.codehaus.groovy.ast.builder.AstBuilder
import org.codehaus.groovy.ast.expr.ArgumentListExpression
import org.codehaus.groovy.ast.expr.ConstantExpression
import org.codehaus.groovy.ast.expr.Expression
import org.codehaus.groovy.ast.expr.MethodCallExpression
import org.codehaus.groovy.ast.expr.PropertyExpression
import org.codehaus.groovy.ast.expr.VariableExpression
import org.codehaus.groovy.control.CompilePhase

/**
 * Processes Gremlin strings as an AST so as to try to detect certain properties from the script without actual
 * having to execute a {@code eval()} on it.
 */
final class GremlinASTChecker {

    private static final astBuilder = new AstBuilder()
    public static final EMPTY_RESULT = new Result(0);

    /**
     * Parses a Gremlin script and extracts a {@code Result} containing properties that are relevant to the checker.
     */
    static Result parse(String gremlin) {
        if (gremlin.empty) return EMPTY_RESULT

        def ast = astBuilder.buildFromString(CompilePhase.CONVERSION, false, gremlin)
        def tocheck = new TimeoutCheck()
        ast[0].visit(tocheck)
        return new Result(tocheck.total)
    }

    static class Result {
        private final long timeout

        Result(long timeout) {
            this.timeout = timeout
        }

        /**
         * Gets the value of the timeouts that were set using the {@code with()} source step. If there are multiple
         * commands using this step, the timeouts are summed together.
         */
        Optional<Long> getTimeout() {
            timeout == 0 ? Optional.empty() : Optional.of(timeout)
        }
    }

    static class TimeoutCheck extends CodeVisitorSupport {

        def total = 0L

        @Override
        void visitMethodCallExpression(MethodCallExpression call) {
            if (call.methodAsString == "with")
                call.getArguments().visit(this)

            call.getObjectExpression().visit(this);
        }

        @Override
        void visitArgumentlistExpression(ArgumentListExpression ale) {
            def argumentExpressions = ale.asList()
            if (argumentExpressions.size() == 2) {
                def key = getArgument(argumentExpressions[0])
                def value = getArgument(argumentExpressions[1])

                if (isTimeoutKey(key) && value instanceof Number) {
                    total += value
                }
            }
        }

        private static isTimeoutKey(def k) {
            // can't use Tokens object from here as it's in gremlin-driver. would rather not put a
            // groovy build in gremlin-driver so we're just stuck with strings for now.
            return k == "evaluationTimeout" ||
                    k == "scriptEvaluationTimeout" ||
                    k == "ARGS_EVAL_TIMEOUT" ||
                    k == "ARGS_SCRIPT_EVAL_TIMEOUT"
        }

        private static getArgument(Expression expr) {
            // ConstantExpression for a direct String usage,
            // PropertyExpression for Token.*
            // VariableExpression for static import of the Token field
            if (expr instanceof ConstantExpression)
                return ((ConstantExpression) expr).value
            else if (expr instanceof PropertyExpression)
                return ((PropertyExpression) expr).propertyAsString
            else if (expr instanceof VariableExpression)
                return ((VariableExpression) expr).text
        }
    }
}

//import org.apache.tinkerpop.gremlin.groovy.jsr223.ast.GremlinASTChecker
//import java.util.concurrent.TimeUnit
//import org.apache.commons.lang.RandomStringUtils
//rand = new java.util.Random()
//engine = new groovy.text.SimpleTemplateEngine()
//baseScript = 'g.with("evaluationTimeout",1).V().out("$a").in().where(outE().has("weight",$b).count().is($c)).order().by("name",desc).limit($d).project("x","y").by("name").by(outE().fold());'
//[1,10,50,100,500,1000,5000,10000].collect {
//    def binding = [a: RandomStringUtils.random(30), b: rand.nextDouble(), c: rand.nextInt(), d: rand.nextInt()]
//    def longScript = (0..<it).collect{engine.createTemplate(baseScript).make(binding)}.join()
//    def start = System.nanoTime()
//    GremlinASTChecker.parse(longScript).getTimeout().get()
//    [it, TimeUnit.MILLISECONDS.convert(System.nanoTime() - start, TimeUnit.NANOSECONDS)]
//}
