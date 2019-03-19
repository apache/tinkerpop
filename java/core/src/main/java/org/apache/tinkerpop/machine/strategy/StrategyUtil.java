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
package org.apache.tinkerpop.machine.strategy;

import org.apache.tinkerpop.util.MultiMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class StrategyUtil {

    private StrategyUtil() {
        // static instance
    }

    private final static List<Class<? extends Strategy>> STRATEGY_CATEGORIES = List.of(
            Strategy.DecorationStrategy.class,
            Strategy.OptimizationStrategy.class,
            Strategy.ProviderStrategy.class,
            Strategy.FinalizationStrategy.class,
            Strategy.VerificationStrategy.class);

    public static Set<Strategy<?>> sortStrategies(final Set<Strategy<?>> strategies) {
        final Map<Class<? extends Strategy>, Set<Class<? extends Strategy>>> dependencyMap = new HashMap<>();
        final Map<Class<? extends Strategy>, Set<Class<? extends Strategy>>> strategiesByCategory = new HashMap<>();
        final Set<Class<? extends Strategy>> strategyClasses = new HashSet<>(strategies.size());
        //Initialize data structure
        strategies.forEach(s -> {
            strategyClasses.add(s.getClass());
            MultiMap.put(strategiesByCategory, s.getStrategyCategory(), s.getClass());
        });

        //Initialize all the dependencies
        strategies.forEach(strategy -> {
            strategy.applyPrior().forEach(s -> {
                if (strategyClasses.contains(s)) MultiMap.put(dependencyMap, strategy.getClass(), s);
            });
            strategy.applyPost().forEach(s -> {
                if (strategyClasses.contains(s)) MultiMap.put(dependencyMap, s, strategy.getClass());
            });
        });

        //Add dependencies by category
        final List<Class<? extends Strategy>> strategiesInPreviousCategories = new ArrayList<>();
        for (Class<? extends Strategy> category : STRATEGY_CATEGORIES) {
            final Set<Class<? extends Strategy>> strategiesInThisCategory = MultiMap.get(strategiesByCategory, category);
            for (Class<? extends Strategy> strategy : strategiesInThisCategory) {
                for (Class<? extends Strategy> previousStrategy : strategiesInPreviousCategories) {
                    MultiMap.put(dependencyMap, strategy, previousStrategy);
                }
            }
            strategiesInPreviousCategories.addAll(strategiesInThisCategory);
        }

        //Finally sort via t-sort
        final List<Class<? extends Strategy>> unprocessedStrategyClasses = new ArrayList<>(strategies.stream().map(s -> s.getClass()).collect(Collectors.toSet()));
        final List<Class<? extends Strategy>> sortedStrategyClasses = new ArrayList<>();
        final Set<Class<? extends Strategy>> seenStrategyClasses = new HashSet<>();

        while (!unprocessedStrategyClasses.isEmpty()) {
            final Class<? extends Strategy> strategy = unprocessedStrategyClasses.get(0);
            visit(dependencyMap, sortedStrategyClasses, seenStrategyClasses, unprocessedStrategyClasses, strategy);
        }

        final Set<Strategy<?>> sortedStrategies = new LinkedHashSet<>();
        //We now have a linked set of sorted strategy classes
        for (Class<? extends Strategy> strategyClass : sortedStrategyClasses) {
            for (Strategy strategy : strategies) {
                if (strategy.getClass().equals(strategyClass)) {
                    sortedStrategies.add(strategy);
                }
            }
        }

        return sortedStrategies;
    }

    private static void visit(final Map<Class<? extends Strategy>, Set<Class<? extends Strategy>>> dependencyMap,
                              final List<Class<? extends Strategy>> sortedStrategyClasses,
                              final Set<Class<? extends Strategy>> seenStrategyClases,
                              final List<Class<? extends Strategy>> unprocessedStrategyClasses, Class<? extends Strategy> strategyClass) {
        if (seenStrategyClases.contains(strategyClass)) {
            throw new IllegalStateException("Cyclic dependency between traversal strategies: [" + seenStrategyClases + ']');
        }

        if (unprocessedStrategyClasses.contains(strategyClass)) {
            seenStrategyClases.add(strategyClass);
            for (Class<? extends Strategy> dependency : MultiMap.get(dependencyMap, strategyClass)) {
                visit(dependencyMap, sortedStrategyClasses, seenStrategyClases, unprocessedStrategyClasses, dependency);
            }
            seenStrategyClases.remove(strategyClass);
            unprocessedStrategyClasses.remove(strategyClass);
            sortedStrategyClasses.add(strategyClass);
        }
    }
}
