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
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.CoinTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.CountTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupCountTest;
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

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class FeatureCoverageTest {

    private static Pattern scenarioName = Pattern.compile("^\\s*Scenario:\\s*(.*)$");

    @Test
    public void shouldImplementAllProcessTestsAsFeatures() throws Exception {

        // TEMPORARY while test framework is under development - all tests should ultimately be included
        final List<Class<?>> temp = Arrays.asList(
                BranchTest.class,
                ChooseTest.class,
                CoinTest.class,
                CountTest.class,
                GroupCountTest.class,
                VertexTest.class);

        final Field field = ProcessStandardSuite.class.getDeclaredField("testsToEnforce");
        field.setAccessible(true);
        final Class<?>[] testsToEnforce = (Class<?>[]) field.get(null);

        final List<Class<?>> testClassesToEnforce = Stream.of(testsToEnforce).filter(temp::contains).collect(Collectors.toList());
        for (Class<?> t : testClassesToEnforce) {
            final String packge = t.getPackage().getName();
            final String group = packge.substring(packge.lastIndexOf(".") + 1, packge.length());
            final String featureFileName = "features" + File.separator +
                                           group + File.separator +
                                           t.getSimpleName().replace("Test", "") + ".feature";
            final Set<String> testMethods = Stream.of(t.getDeclaredMethods())
                    .filter(m -> m.isAnnotationPresent(Test.class))
                    .map(Method::getName).collect(Collectors.toSet());

            final File featureFile = new File(featureFileName);
            assertThat(featureFile.exists(), is(true));
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

            assertEquals("All test methods are not implemented in the " + featureFileName + ": " + testMethods, testMethods.size(), 0);
        }
    }
}
