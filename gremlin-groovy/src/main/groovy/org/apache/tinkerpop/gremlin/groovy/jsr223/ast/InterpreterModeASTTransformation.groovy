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

import groovy.transform.CompileStatic
import org.apache.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine
import org.codehaus.groovy.ast.ASTNode
import org.codehaus.groovy.ast.ClassHelper
import org.codehaus.groovy.ast.ClassNode
import org.codehaus.groovy.ast.MethodNode
import org.codehaus.groovy.ast.Parameter
import org.codehaus.groovy.ast.expr.ArgumentListExpression
import org.codehaus.groovy.ast.expr.BinaryExpression
import org.codehaus.groovy.ast.expr.ConstantExpression
import org.codehaus.groovy.ast.expr.DeclarationExpression
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
            def runMethodOfScript = scriptNode.declaredMethodsMap["java.lang.Object run()"]
            runMethodOfScript.code = wrap(runMethodOfScript)
        }
    }

    private static BlockStatement wrap(MethodNode method) {
        BlockStatement wrappedBlock = new BlockStatement()
        BlockStatement existingBlock = ((BlockStatement) method.code)

        // the variable names that will be written back to the global context
        def variableNames = [] as Set<String>
        variableNames.addAll(findTopLevelVariableDeclarations(existingBlock.statements))
        method.variableScope.referencedClassVariablesIterator.each{variableNames << it.name}

        // the map to hold the variables and values
        wrappedBlock.addStatement(createGlobalMapAST())

        // the finally block will capture all the vars in the "globals" map
        BlockStatement finallyBlock = new BlockStatement()
        variableNames.each {
            finallyBlock.addStatement(createAssignToGlobalMapAST(it))
        }

        wrappedBlock.addStatement(new TryCatchStatement(existingBlock, finallyBlock))

        return wrappedBlock
    }

    private static List<String> findTopLevelVariableDeclarations(def existingStatements) {
        existingStatements.findAll{ it instanceof ExpressionStatement }
                .collect{ ((ExpressionStatement) it).expression }
                .findAll{ it instanceof DeclarationExpression}
                .collect{ ((DeclarationExpression) it).leftExpression }
                .collect{ ((VariableExpression) it).name }
    }

    private static Statement createAssignToGlobalMapAST(String varName) {
        def tryCatch = new TryCatchStatement(new ExpressionStatement(
                new MethodCallExpression(
                        new VariableExpression(GremlinGroovyScriptEngine.COLLECTED_BOUND_VARS_MAP_VARNAME),
                        "put",
                        new ArgumentListExpression(new ConstantExpression(varName), new VariableExpression(varName)))), EmptyStatement.INSTANCE)

        tryCatch.addCatch(new CatchStatement(new Parameter(ClassHelper.make(MissingPropertyException), "ex"), EmptyStatement.INSTANCE))
        return tryCatch
    }

    private static Statement createGlobalMapAST() {
        new ExpressionStatement(
                new BinaryExpression(
                        new VariableExpression(GremlinGroovyScriptEngine.COLLECTED_BOUND_VARS_MAP_VARNAME),
                        Token.newSymbol(Types.EQUAL, 0, 0),
                        new MapExpression()))
    }
}
