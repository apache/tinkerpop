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
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MaxTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MeanTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MinTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PageRankTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PathTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.ProjectTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertiesTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SumTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.UnfoldTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.ValueMapTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.AggregateTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupCountTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.InjectTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SackTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SideEffectCapTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SideEffectTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.StoreTest;
import org.junit.Ignore;
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

    private static final List<String> testToIgnore = Arrays.asList(
            // deprecated tests
            "g_V_addVXlabel_animal_age_0X",
            "g_addVXlabel_person_name_stephenX",
            // GLV suite doesn't support property identifiers and related assertions
            "g_V_hasXageX_properties_hasXid_nameIdX_value",
            "g_V_hasXageX_properties_hasXid_nameIdAsStringX_value",
            "g_V_hasXageX_propertiesXnameX",
            // grateful dead graph not supported in GLV suite
            "g_V_matchXa_0sungBy_b__a_0writtenBy_c__b_writtenBy_d__c_sungBy_d__d_hasXname_GarciaXX",
            "g_V_matchXa_hasXsong_name_sunshineX__a_mapX0followedBy_weight_meanX_b__a_0followedBy_c__c_filterXweight_whereXgteXbXXX_outV_dX_selectXdX_byXnameX",
            "g_V_matchXa_0sungBy_b__a_0sungBy_c__b_writtenBy_d__c_writtenBy_e__d_hasXname_George_HarisonX__e_hasXname_Bob_MarleyXX",
            "g_V_matchXa_hasXname_GarciaX__a_0writtenBy_b__a_0sungBy_bX",
            "g_V_hasLabelXsongsX_matchXa_name_b__a_performances_cX_selectXb_cX_count",
            "g_V_matchXa_hasXname_GarciaX__a_0writtenBy_b__b_followedBy_c__c_writtenBy_d__whereXd_neqXaXXX",
            "g_V_matchXa_0sungBy_b__a_0writtenBy_c__b_writtenBy_dX_whereXc_sungBy_dX_whereXd_hasXname_GarciaXX",
            "get_g_V_matchXa_followedBy_count_isXgtX10XX_b__a_0followedBy_count_isXgtX10XX_bX_count",
            "g_V_matchXa_followedBy_count_isXgtX10XX_b__a_0followedBy_count_isXgtX10XX_bX_count",
            "g_V_hasXsong_name_OHBOYX_outXfollowedByX_outXfollowedByX_order_byXperformancesX_byXsongType_descX",
            "g_V_hasLabelXsongX_order_byXperformances_descX_byXnameX_rangeX110_120X_name",
            // Pop tests not organized right for GLVs
            "g_V_valueMap_selectXpop_aX",
            "g_V_selectXa_bX",
            "g_V_valueMap_selectXpop_a_bX",
            "g_V_selectXaX",
            // assertion doesn't seem to want to work right for embedded lists
            "g_V_asXa_bX_out_asXcX_path_selectXkeysX",
            // probably need TINKERPOP-1877
            "g_V_bothEXselfX",
            "g_V_bothXselfX",
            // ugh - BigInteger?
            "g_withSackXBigInteger_TEN_powX1000X_assignX_V_localXoutXknowsX_barrierXnormSackXX_inXknowsX_barrier_sack",
            // ugh - clone
            "g_withSackXmap__map_cloneX_V_out_out_sackXmap_a_nameX_sack",
            // wont round right or something
            "g_withSackX2X_V_sackXdivX_byXconstantX3_0XX_sack");

    @Test
    // @Ignore("As it stands we won't have all of these tests migrated initially so there is no point to running this in full - it can be flipped on later")
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
                MatchTest.class,
                MaxTest.class,
                MeanTest.class,
                MinTest.class,
                OrderTest.class,
                PageRankTest.class,
                PathTest.class,
                // PeerPressureTest.class,
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
                SackTest.class,
                SideEffectCapTest.class,
                //SideEffectTest.class,
                StoreTest.class);
                // SubgraphTest.class,
                // TreeTest.class);

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
                    .filter(m -> !testToIgnore.contains(m.getName()))
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
