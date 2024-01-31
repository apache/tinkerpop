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

import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.util.GremlinDisabledListDelimiterHandler;

import java.util.List;
import java.util.Optional;

public class TraversalStrategyVisitor extends DefaultGremlinBaseVisitor<TraversalStrategy> {
    protected final GremlinAntlrToJava antlr;

    public TraversalStrategyVisitor(final GremlinAntlrToJava antlrToJava) {
        this.antlr = antlrToJava;
    }

    @Override
    public TraversalStrategy visitTraversalStrategy(final GremlinParser.TraversalStrategyContext ctx) {
        // child count of one implies init syntax for the singleton constructed strategies. otherwise, it will
        // fall back to the Builder methods for construction
        if (ctx.getChildCount() == 1) {
            final String strategyName = ctx.getChild(0).getText();
            return tryToConstructStrategy(strategyName, getConfiguration(ctx.configuration()));
        } else {
            // start looking at strategies after the "new" keyword
            final int childIndex = ctx.getChild(0).getText().equals("new") ? 1 : 0;
            final String strategyName = ctx.getChild(childIndex).getText();
            return tryToConstructStrategy(strategyName, getConfiguration(ctx.configuration()));
        }
    }

    /**
     * Builds a {@code Configuration} object from the arguments given to the strategy.
     */
    private Configuration getConfiguration(final List<GremlinParser.ConfigurationContext> contexts) {
        final BaseConfiguration conf = new BaseConfiguration();
        conf.setListDelimiterHandler(GremlinDisabledListDelimiterHandler.instance());
        if (null != contexts) {
            for (GremlinParser.ConfigurationContext ctx : contexts) {
                final String key = ctx.getChild(0).getText();
                final Object val = antlr.argumentVisitor.visitGenericLiteralArgument(ctx.genericLiteralArgument());
                conf.setProperty(key, val);
            }
        }
        return conf;
    }

    /**
     * Try to instantiate the strategy by checking registered {@link TraversalStrategy} implementations that are
     * registered globally. Only strategies that are registered globally can be constructed in this way.
     */
    private static TraversalStrategy tryToConstructStrategy(final String strategyName, final Configuration conf) {
        // try to grab the strategy class from registered sources
        final Optional<? extends Class<? extends TraversalStrategy>> opt = TraversalStrategies.GlobalCache.getRegisteredStrategyClass(strategyName);

        if (!opt.isPresent())
            throw new IllegalStateException("TraversalStrategy not recognized - " + strategyName);

        final Class clazz = opt.get();
        try {
            // if there is no configuration then we can use the instance() method and if that fails the public
            // constructor, followed by the standard create(). otherwise we need to pass the Configuration to the
            // create() method
            if (conf.isEmpty()) {
                try {
                    return (TraversalStrategy) clazz.getMethod("instance").invoke(null);
                } catch (Exception ex) {
                    try {
                        return (TraversalStrategy) clazz.getConstructor().newInstance();
                    } catch (Exception exinner) {
                        return (TraversalStrategy) clazz.getMethod("create", Configuration.class).invoke(null, conf);
                    }
                }
            } else {
                return (TraversalStrategy) clazz.getMethod("create", Configuration.class).invoke(null, conf);
            }
        } catch (Exception ex) {
            throw new IllegalStateException("TraversalStrategy not recognized - " + strategyName, ex);
        }
    }
}
