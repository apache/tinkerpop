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
package org.apache.tinkerpop.gremlin.process;

import org.apache.tinkerpop.gremlin.process.traversal.step.branch.BranchTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.ChooseTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.LocalTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.OptionalTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.UnionTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.AndTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.CoinTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.CyclicPathTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.DedupTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.DropTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.FilterTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.IsTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.OrTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.SampleTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.SimplePathTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.TailTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddEdgeTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddVertexTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.CoalesceTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.ConstantTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.CountTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.FlatMapTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.FoldTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.LoopsTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MapTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MatchTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MathTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MaxTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MeanTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MinTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PageRankTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PathTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PeerPressureTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.ProjectTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertiesTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.ReadTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SumTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.UnfoldTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.ValueMapTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.WriteTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.AggregateTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupCountTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.InjectTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SackTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SideEffectCapTest;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@org.junit.Ignore("Don't think we need to keep these in sync anymore, but not quite ready to delete this all together")
public class FeatureCoverageTest {

    private static Pattern scenarioName = Pattern.compile("^\\s*Scenario:\\s*(.*)$");

    @Test
    public void shouldImplementAllProcessTestsAsFeatures() throws Exception {

        // TEMPORARY while test framework is under development - all tests should ultimately be included
        final List<Class<?>> temp = Arrays.asList(
                // branch
                BranchTest.class,
                ChooseTest.class,
                LocalTest.class,
                OptionalTest.class,
                RepeatTest.class,
                UnionTest.class,
                // filter
                AndTest.class,
                CoinTest.class,
                CyclicPathTest.class,
                DedupTest.class,
                DropTest.class,
                FilterTest.class,
                HasTest.class,
                IsTest.class,
                OrTest.class,
                RangeTest.class,
                SampleTest.class,
                SimplePathTest.class,
                TailTest.class,
                WhereTest.class,
                // map
                AddEdgeTest.class,
                AddVertexTest.class,
                CoalesceTest.class,
                ConstantTest.class,
                CountTest.class,
                FlatMapTest.class,
                FoldTest.class,
                GraphTest.class,
                LoopsTest.class,
                MapTest.class,
                MathTest.class,
                MatchTest.class,
                MaxTest.class,
                MeanTest.class,
                MinTest.class,
                OrderTest.class,
                PageRankTest.class,
                PathTest.class,
                PeerPressureTest.class,
                // ProfileTest.class,
                // ProgramTest.class,
                ProjectTest.class,
                PropertiesTest.class,
                SelectTest.class,
                SumTest.class,
                UnfoldTest.class,
                ValueMapTest.class,
                VertexTest.class,
                // sideEffect
                AggregateTest.class,
                // ExplainTest.class,
                GroupCountTest.class,
                GroupTest.class,
                InjectTest.class,
                ReadTest.class,
                SackTest.class,
                SideEffectCapTest.class,
                //SideEffectTest.class,
                WriteTest.class);
                // SubgraphTest.class,
                // TreeTest.class);

        final Field field = ProcessStandardSuite.class.getDeclaredField("testsToEnforce");
        field.setAccessible(true);
        final Class<?>[] testsToEnforce = (Class<?>[]) field.get(null);

        final List<Class<?>> testClassesToEnforce = Stream.of(testsToEnforce).filter(temp::contains).collect(Collectors.toList());
        for (Class<?> t : testClassesToEnforce) {
            final String packge = t.getPackage().getName();
            final String group = packge.substring(packge.lastIndexOf(".") + 1, packge.length());
            final String featureFileName = "org/apache/tinkerpop/gremlin/test/features" + File.separator +
                                           group + File.separator +
                                           t.getSimpleName().replace("Test", "") + ".feature";
            final Set<String> testMethods = Stream.of(t.getDeclaredMethods())
                    .filter(m -> m.isAnnotationPresent(Test.class))
                    .map(Method::getName).collect(Collectors.toSet());

            final File featureFile = new File(featureFileName);
            assertThat("Where is: " + featureFileName, featureFile.exists(), is(true));
            assertThat(featureFile.isFile(), is(true));

            final Set<String> testsInFeatureFile = new HashSet<>();
            final InputStream is = new FileInputStream(featureFile);
            final BufferedReader buf = new BufferedReader(new InputStreamReader(is));
            String line = buf.readLine();
            while(line != null){
                final Matcher matcher = scenarioName.matcher(line);
                if (matcher.matches())
                    testsInFeatureFile.add(matcher.group(1));
                line = buf.readLine();
            }

            testMethods.removeAll(testsInFeatureFile);

            assertEquals("All test methods are not implemented in the " + featureFileName + ": " + testMethods, 0, testMethods.size());
        }
    }
}
