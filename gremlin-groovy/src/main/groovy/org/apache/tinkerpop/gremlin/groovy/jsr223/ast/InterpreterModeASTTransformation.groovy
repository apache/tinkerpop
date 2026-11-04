/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.groovy.jsr223.ast

import groovy.transform.CompileStatic
import org.apache.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine
import org.codehaus.groovy.ast.ASTNode
import org.codehaus.groovy.ast.ClassHelper
import org.codehaus.groovy.ast.ClassNode
import org.codehaus.groovy.ast.DynamicVariable
import org.codehaus.groovy.ast.MethodNode
import org.codehaus.groovy.ast.Parameter
import org.codehaus.groovy.ast.Variable
import org.codehaus.groovy.ast.expr.ArgumentListExpression
import org.codehaus.groovy.ast.expr.BinaryExpression
import org.codehaus.groovy.ast.expr.ConstantExpression
import org.codehaus.groovy.ast.expr.DeclarationExpression
import org.codehaus.groovy.ast.expr.EmptyExpression
import org.codehaus.groovy.ast.expr.MapExpression
import org.codehaus.groovy.ast.expr.MethodCallExpression
import org.codehaus.groovy.ast.expr.VariableExpression
import org.codehaus.groovy.ast.stmt.BlockStatement
import org.codehaus.groovy.ast.stmt.CatchStatement
import org.codehaus.groovy.ast.stmt.EmptyStatement
import org.codehaus.groovy.ast.stmt.ExpressionStatement
import org.codehaus.groovy.ast.stmt.Statement
import org.codehaus.groovy.ast.stmt.TryCatchStatement
import org.codehaus.groovy.control.CompilePhase
import org.codehaus.groovy.control.SourceUnit
import org.codehaus.groovy.syntax.Token
import org.codehaus.groovy.syntax.Types
import org.codehaus.groovy.transform.ASTTransformation
import org.codehaus.groovy.transform.GroovyASTTransformation

/**
 * An {@code ASTTransformation} that promotes "local" variables to global ones.  In this case, "local" refers to those
 * variables that are defined in a script with "def" at the root of the script.  These would typically be interpreted
 * as local to the script, but this transform changes that, by wrapping the entire script in a try/catch where such
 * variables are written to a "hidden" {@link Map} so that the {@code ScriptEngine} can later access them to place
 * them into the global context.
 */
@CompileStatic
@GroovyASTTransformation(phase=CompilePhase.SEMANTIC_ANALYSIS)
class InterpreterModeASTTransformation implements ASTTransformation {

    @Override
    void visit(ASTNode[] astNodes, SourceUnit sourceUnit) {
        ClassNode scriptNode = (ClassNode) astNodes[1]

        // need to check that object is a Script to call run(). this scriptNode may be a user defined class via
        // "def class" in which case it can be ignored as there are no variables to promote to global status there
        if (scriptNode.isDerivedFrom(ClassHelper.make(Script))) {
            def runMethodOfScript = scriptNode.getDeclaredMethod("run", Parameter.EMPTY_ARRAY)
            if (runMethodOfScript != null)
                runMethodOfScript.code = wrap(runMethodOfScript)
        }
    }

    private static BlockStatement wrap(MethodNode method) {
        BlockStatement wrappedBlock = new BlockStatement()
        BlockStatement existingBlock = ((BlockStatement) method.code)

        // generated references are linked to their Variable as scopes are already resolved at this phase
        def variables = [:] as LinkedHashMap<String, Variable>
        def hoisted = hoistTopLevelVariableDeclarations(existingBlock)
        hoisted.each { variables[it.name] = (Variable) it }
        method.variableScope.referencedClassVariablesIterator.each { Variable v ->
            if (!variables.containsKey(v.name)) variables[v.name] = v
        }

        // declare outside the try so the finally block can see them
        wrappedBlock.variableScope = existingBlock.variableScope
        hoisted.each { VariableExpression v ->
            wrappedBlock.addStatement(new ExpressionStatement(
                    new DeclarationExpression(v, Token.newSymbol(Types.EQUAL, 0, 0), EmptyExpression.INSTANCE)))
        }

        // the map to hold the variables and values
        wrappedBlock.addStatement(createGlobalMapAST())

        // the finally block will capture all the vars in the "globals" map
        BlockStatement finallyBlock = new BlockStatement()
        variables.each { String name, Variable declared ->
            finallyBlock.addStatement(createAssignToGlobalMapAST(name, declared))
        }

        wrappedBlock.addStatement(new TryCatchStatement(existingBlock, finallyBlock))

        return wrappedBlock
    }

    /**
     * Turns each top level {@code def x = value} into {@code x = value} and returns the declared variables.
     */
    private static List<VariableExpression> hoistTopLevelVariableDeclarations(final BlockStatement block) {
        def hoisted = [] as List<VariableExpression>
        block.statements.each { Statement statement ->
            if (!(statement instanceof ExpressionStatement)) return
            def expressionStatement = (ExpressionStatement) statement
            if (!(expressionStatement.expression instanceof DeclarationExpression)) return
            def declaration = (DeclarationExpression) expressionStatement.expression
            if (!(declaration.leftExpression instanceof VariableExpression)) return

            def variable = (VariableExpression) declaration.leftExpression
            hoisted << variable
            expressionStatement.expression = declaration.rightExpression instanceof EmptyExpression ?
                    new VariableExpression(variable) :
                    new BinaryExpression(new VariableExpression(variable), Token.newSymbol(Types.ASSIGN, 0, 0),
                            declaration.rightExpression)
        }
        return hoisted
    }

    private static Statement createAssignToGlobalMapAST(String varName, Variable variable) {
        def tryCatch = new TryCatchStatement(new ExpressionStatement(
                new MethodCallExpression(
                        globalMapVariable(),
                        "put",
                        new ArgumentListExpression(new ConstantExpression(varName), new VariableExpression(variable)))), EmptyStatement.INSTANCE)

        tryCatch.addCatch(new CatchStatement(new Parameter(ClassHelper.make(MissingPropertyException), "ex"), EmptyStatement.INSTANCE))
        return tryCatch
    }

    private static Statement createGlobalMapAST() {
        new ExpressionStatement(
                new BinaryExpression(
                        globalMapVariable(),
                        Token.newSymbol(Types.EQUAL, 0, 0),
                        new MapExpression()))
    }

    private static VariableExpression globalMapVariable() {
        new VariableExpression(new DynamicVariable(GremlinGroovyScriptEngine.COLLECTED_BOUND_VARS_MAP_VARNAME, false))
    }
}
