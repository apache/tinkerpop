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

package org.apache.tinkerpop.gremlin.python.jsr223;

import org.apache.commons.configuration.ConfigurationConverter;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.TraversalStrategyProxy;
import org.apache.tinkerpop.gremlin.util.function.Lambda;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class JythonTranslator extends PythonTranslator {

    private JythonTranslator(final String traversalSource, final boolean importStatics) {
        super(traversalSource, importStatics);
    }

    public static JythonTranslator of(final String traversalSource) {
        return new JythonTranslator(traversalSource, false);
    }

    @Override
    public String getTargetLanguage() {
        return "gremlin-jython";
    }

    @Override
    protected String convertLambdaToString(final Lambda lambda) {
        String lambdaString = lambda.getLambdaScript().trim();
        lambdaString = lambdaString.startsWith("lambda") ?
                lambdaString :
                "lambda " + lambdaString;
        if (0 == lambda.getLambdaArguments())
            return "JythonZeroArgLambda(" + lambdaString + ")";
        else if (1 == lambda.getLambdaArguments())
            return "JythonOneArgLambda(" + lambdaString + ")";
        else if (2 == lambda.getLambdaArguments())
            return "JythonTwoArgLambda(" + lambdaString + ")";
        else
            return "JythonUnknownArgLambda(" + lambdaString + ")";
    }

    @Override
    protected String resolveSymbol(final String methodName) {
        // since this is Jython we should expect the Gremlin to conform to the java classes to which the engine is
        // bound - therefore, unlike the python engine which converts java names to python friendly ones, this
        // jython one can just pass them through.
        return methodName;
    }

    @Override
    protected String resolveTraversalStrategyProxy(final TraversalStrategyProxy proxy) {
        // since this is jython we don't need a traversal proxy here - we need the actual JVM version of the strategy
        // since this script will be executed in Jython. 
        if (proxy.getConfiguration().isEmpty())
            return proxy.getStrategyClass().getCanonicalName() + ".instance()";
        else
            return proxy.getStrategyClass().getCanonicalName() + ".create(org.apache.commons.configuration.MapConfiguration(" + convertToString(ConfigurationConverter.getMap(proxy.getConfiguration())) + "))";
    }
}
