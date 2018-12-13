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

package org.apache.tinkerpop.gremlin.groovy.jsr223;

import org.apache.tinkerpop.gremlin.GraphProvider;
import org.apache.tinkerpop.gremlin.TestHelper;
import org.apache.tinkerpop.gremlin.process.computer.Computer;
import org.apache.tinkerpop.gremlin.structure.RemoteGraph;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.process.computer.TinkerGraphComputer;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.AddEdgeTest",
        method = "*",
        reason = "The addEdge() step is not supported by GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.AndTest",
        method = "g_V_asXaX_outXknowsX_and_outXcreatedX_inXcreatedX_asXaX_name",
        reason = "Local traversals may not traverse past the local star-graph on GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.ChooseTest",
        method = "g_injectX1X_chooseXisX1X__constantX10Xfold__foldX",
        reason = "The inject() step is not supported by GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.ChooseTest",
        method = "g_injectX2X_chooseXisX1X__constantX10Xfold__foldX",
        reason = "The inject() step is not supported by GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.CoalesceTest",
        method = "g_V_coalesceXoutEXknowsX_outEXcreatedXX_otherV_path_byXnameX_byXlabelX",
        reason = "It is not possible to access more than a path element's id on GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.ComplexTest",
        method = "playlistPaths",
        reason = "Local traversals may not traverse past the local star-graph on GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.CountTest",
        method = "g_V_whereXinXkknowsX_outXcreatedX_count_is_0XX_name",
        reason = "Local traversals may not traverse past the local star-graph on GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.DedupTest",
        method = "g_V_asXaX_both_asXbX_dedupXa_bX_byXlabelX_selectXa_bX",
        reason = "It is not possible to access more than a path element's id on GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphTest",
        method = "g_V_hasLabelXpersonX_asXpX_VXsoftwareX_addInEXuses_pX",
        reason = "Mid-traversal V()/E() is currently not supported on GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphTest",
        method = "g_V_hasXname_GarciaX_inXsungByX_asXsongX_V_hasXname_Willie_DixonX_inXwrittenByX_whereXeqXsongXX_name",
        reason = "Mid-traversal V()/E() is currently not supported on GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphTest",
        method = "g_V_outXknowsX_V_name",
        reason = "Mid-traversal V()/E() is currently not supported on GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphTest",
        method = "g_VX1X_V_valuesXnameX",
        reason = "Mid-traversal V()/E() is currently not supported on GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupTest",
        method = "g_V_groupXmX_byXnameX_byXinXknowsX_nameX_capXmX",
        reason = "Local traversals may not traverse past the local star-graph on GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupTest",
        method = "g_V_hasLabelXpersonX_asXpX_outXcreatedX_group_byXnameX_byXselectXpX_valuesXageX_sumX",
        reason = "Local traversals may not traverse past the local star-graph on GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.InjectTest",
        method = "g_VX1X_injectXg_VX4XX_out_name",
        reason = "The inject() step is not supported by GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.InjectTest",
        method = "g_VX1X_out_injectXv2X_name",
        reason = "The inject() step is not supported by GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.LocalTest",
        method = "g_V_localXmatchXproject__created_person__person_name_nameX_selectXname_projectX_by_byXnameX",
        reason = "Local traversals may not traverse past the local star-graph on GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.LoopsTest",
        method = "g_VX1X_repeatXboth_simplePathX_untilXhasXname_peterX_and_loops_isX3XX_hasXname_peterX_path_byXnameX",
        reason = "It is not possible to access more than a path element's id on GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.LoopsTest",
        method = "g_VX1X_repeatXboth_simplePathX_untilXhasXname_peterX_or_loops_isX2XX_hasXname_peterX_path_byXnameX",
        reason = "It is not possible to access more than a path element's id on GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.LoopsTest",
        method = "g_VX1X_repeatXboth_simplePathX_untilXhasXname_peterX_or_loops_isX3XX_hasXname_peterX_path_byXnameX",
        reason = "It is not possible to access more than a path element's id on GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.MatchTest",
        method = "g_V_matchXa_both_b__b_both_cX_dedupXa_bX_byXlabelX",
        reason = "It is not possible to access more than a path element's id on GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.MatchTest",
        method = "g_V_matchXa_created_lop_b__b_0created_29_c__c_whereXrepeatXoutX_timesX2XXX",
        reason = "It is not possible to access more than a path element's id on GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.MatchTest",
        method = "g_V_matchXa_created_lop_b__b_0created_29_cX_whereXc_repeatXoutX_timesX2XX_selectXa_b_cX",
        reason = "Local traversals may not traverse past the local star-graph on GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.MatchTest",
        method = "g_V_matchXwhereXandXa_created_b__b_0created_count_isXeqX3XXXX__a_both_b__whereXb_inXX",
        reason = "It is not possible to access more than a path element's id on GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.MatchTest",
        method = "g_V_matchXa_both_b__b_both_cX_dedupXa_bX_byXlabelX",
        reason = "It is not possible to access more than a path element's id on GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.MatchTest",
        method = "g_V_matchXa_created_lop_b__b_0created_29_c__c_whereXrepeatXoutX_timesX2XXX",
        reason = "Local traversals may not traverse past the local star-graph on GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.MatchTest",
        method = "g_V_matchXa_created_lop_b__b_0created_29_cX_whereXc_repeatXoutX_timesX2XX_selectXa_b_cX",
        reason = "Local traversals may not traverse past the local star-graph on GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.MatchTest",
        method = "g_V_matchXwhereXandXa_created_b__b_0created_count_isXeqX3XXXX__a_both_b__whereXb_inXX",
        reason = "It is not possible to access more than a path element's id on GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.MathTest",
        method = "g_V_asXaX_outXcreatedX_asXbX_mathXb_plus_aX_byXinXcreatedX_countX_byXageX",
        reason = "It is not possible to access more than a path element's id on GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.MathTest",
        method = "g_V_asXaX_outXknowsX_asXbX_mathXa_plus_bX_byXageX",
        reason = "It is not possible to access more than a path element's id on GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.MathTest",
        method = "g_withSackX1X_injectX1X_repeatXsackXsumX_byXconstantX1XXX_timesX5X_emit_mathXsin__X_byXsackX",
        reason = "The inject() step is not supported by GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.MinTest",
        method = "g_V_foo_injectX9999999999X_min",
        reason = "The inject() step is not supported by GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.OptionalTest",
        method = "g_V_hasLabelXpersonX_optionalXoutXknowsX_optionalXoutXcreatedXXX_path",
        reason = "Local traversals may not traverse past the local star-graph on GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.OptionalTest",
        method = "g_V_optionalXout_optionalXoutXX_path",
        reason = "Local traversals may not traverse past the local star-graph on GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.OptionalTest",
        method = "g_VX1X_optionalXaddVXdogXX_label",
        reason = "The addV() step is not supported on GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.SimplePathTest",
        method = "g_V_asXaX_out_asXbX_out_asXcX_simplePath_byXlabelX_fromXbX_toXcX_path_byXnameX",
        reason = "It is not possible to access more than a path element's id on GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SubgraphStrategyProcessTest",
        method = "shouldFilterComplexVertexCriterion",
        reason = "Local traversals may not traverse past the local star-graph on GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SubgraphStrategyProcessTest",
        method = "shouldFilterEdgeCriterion",
        reason = "Local traversals may not traverse past the local star-graph on GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SubgraphStrategyProcessTest",
        method = "shouldFilterMixedCriteria",
        reason = "Local traversals may not traverse past the local star-graph on GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SubgraphStrategyProcessTest",
        method = "shouldFilterVertexCriterion",
        reason = "Local traversals may not traverse past the local star-graph on GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SubgraphStrategyProcessTest",
        method = "shouldFilterVertexCriterionAndKeepLabels",
        reason = "Local traversals may not traverse past the local star-graph on GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SubgraphStrategyProcessTest",
        method = "shouldGetExcludedEdge",
        reason = "Local traversals may not traverse past the local star-graph on GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.TailTest",
        method = "g_V_asXaX_out_asXaX_out_asXaX_selectXmixed_aX_byXlimitXlocal_0XX_tailXlocal_1X",
        reason = "It is not possible to access more than a path element's id on GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.TailTest",
        method = "g_V_asXaX_out_asXaX_out_asXaX_selectXmixed_aX_byXunfold_valuesXnameX_foldX_tailXlocal_1X",
        reason = "It is not possible to access more than a path element's id on GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.TailTest",
        method = "g_V_asXaX_out_asXaX_out_asXaX_selectXmixed_aX_byXunfold_valuesXnameX_foldX_tailXlocal_2X",
        reason = "It is not possible to access more than a path element's id on GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.TailTest",
        method = "g_V_asXaX_out_asXaX_out_asXaX_selectXmixed_aX_byXunfold_valuesXnameX_foldX_tailXlocalX",
        reason = "It is not possible to access more than a path element's id on GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.TreeTest",
        method = "g_VX1X_out_out_tree_byXnameX",
        reason = "It is not possible to access more than a path element's id on GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.TreeTest",
        method = "g_VX1X_out_out_treeXaX_byXnameX_both_both_capXaX",
        reason = "It is not possible to access more than a path element's id on GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.UnfoldTest",
        method = "g_VX1X_repeatXboth_simplePathX_untilXhasIdX6XX_path_byXnameX_unfold",
        reason = "It is not possible to access more than a path element's id on GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexTest",
        method = "g_V_hasLabelXpersonX_V_hasLabelXsoftwareX_name",
        reason = "Mid-traversal V()/E() is currently not supported on GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereTest",
        method = "g_V_asXaX_out_asXbX_whereXandXasXaX_outXknowsX_asXbX__orXasXbX_outXcreatedX_hasXname_rippleX__asXbX_inXknowsX_count_isXnotXeqX0XXXXX_selectXa_bX",
        reason = "It is not possible to access more than a path element's id on GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereTest",
        method = "g_V_asXaX_out_asXbX_whereXin_count_isXeqX3XX_or_whereXoutXcreatedX_and_hasXlabel_personXXX_selectXa_bX",
        reason = "It is not possible to access more than a path element's id on GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereTest",
        method = "g_V_asXaX_outEXcreatedX_asXbX_inV_asXcX_inXcreatedX_asXdX_whereXa_ltXbX_orXgtXcXX_andXneqXdXXX_byXageX_byXweightX_byXinXcreatedX_valuesXageX_minX_selectXa_c_dX",
        reason = "It is not possible to access more than a path element's id on GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereTest",
        method = "g_V_asXaX_outEXcreatedX_asXbX_inV_asXcX_whereXa_gtXbX_orXeqXbXXX_byXageX_byXweightX_byXweightX_selectXa_cX_byXnameX",
        reason = "It is not possible to access more than a path element's id on GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereTest",
        method = "g_V_asXaX_outXcreatedX_asXbX_whereXandXasXbX_in__notXasXaX_outXcreatedX_hasXname_rippleXXX_selectXa_bX",
        reason = "It is not possible to access more than a path element's id on GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereTest",
        method = "g_V_asXaX_outXcreatedX_inXcreatedX_asXbX_whereXa_gtXbXX_byXageX_selectXa_bX_byXnameX",
        reason = "It is not possible to access more than a path element's id on GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereTest",
        method = "g_VX1X_asXaX_out_hasXageX_whereXgtXaXX_byXageX_name",
        reason = "It is not possible to access more than a path element's id on GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereTest",
        method = "g_VX1X_asXaX_outXcreatedX_inXcreatedX_asXbX_whereXasXbX_outXcreatedX_hasXname_rippleXX_valuesXage_nameX",
        reason = "Local traversals may not traverse past the local star-graph on GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.PathTest",
        method = "g_V_asXaX_out_asXbX_out_asXcX_path_fromXbX_toXcX_byXnameX",
        reason = "It is not possible to access more than a path element's id on GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.PathTest",
        method = "g_V_out_out_path_byXnameX_byXageX",
        reason = "It is not possible to access more than a path element's id on GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.PathTest",
        method = "g_V_repeatXoutX_timesX2X_path_byXitX_byXnameX_byXlangX",
        reason = "It is not possible to access more than a path element's id on GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.PathTest",
        method = "g_VX1X_out_path_byXageX_byXnameX",
        reason = "It is not possible to access more than a path element's id on GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeTest",
        method = "g_V_asXaX_in_asXaX_in_asXaX_selectXmixed_aX_byXunfold_valuesXnameX_foldX_limitXlocal_1X",
        reason = "It is not possible to access more than a path element's id on GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeTest",
        method = "g_V_asXaX_in_asXaX_in_asXaX_selectXmixed_aX_byXunfold_valuesXnameX_foldX_limitXlocal_2X",
        reason = "It is not possible to access more than a path element's id on GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeTest",
        method = "g_V_asXaX_out_asXaX_out_asXaX_selectXmixed_aX_byXunfold_valuesXnameX_foldX_rangeXlocal_1_2X",
        reason = "It is not possible to access more than a path element's id on GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeTest",
        method = "g_V_asXaX_out_asXaX_out_asXaX_selectXmixed_aX_byXunfold_valuesXnameX_foldX_rangeXlocal_1_3X",
        reason = "It is not possible to access more than a path element's id on GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeTest",
        method = "g_V_asXaX_out_asXaX_out_asXaX_selectXmixed_aX_byXunfold_valuesXnameX_foldX_rangeXlocal_4_5X",
        reason = "It is not possible to access more than a path element's id on GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatTest",
        method = "g_V_hasXloop_name_loopX_repeatXinX_timesX5X_path_by_name",
        reason = "It is not possible to access more than a path element's id on GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatTest",
        method = "g_V_hasXname_markoX_repeatXoutE_inV_simplePathX_untilXhasXname_rippleXX_path_byXnameX_byXlabelX",
        reason = "It is not possible to access more than a path element's id on GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SackTest",
        method = "g_withBulkXfalseX_withSackX1_sumX_V_out_barrier_sack",
        reason = "One bulk is currently not supported on GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SackTest",
        method = "g_withBulkXfalseX_withSackX1_sumX_VX1X_localXoutEXknowsX_barrierXnormSackX_inVX_inXknowsX_barrier_sack",
        reason = "One bulk is currently not supported on GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest",
        method = "g_V_asXaX_groupXmX_by_byXbothE_countX_barrier_selectXmX_selectXselectXaXX_byXmathX_plus_XX",
        reason = "It is not possible to access more than a path element's id on GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest",
        method = "g_V_asXaX_outXknowsX_asXbX_localXselectXa_bX_byXnameXX",
        reason = "It is not possible to access more than a path element's id on GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest",
        method = "g_V_hasLabelXsoftwareX_asXnameX_asXlanguageX_asXcreatorsX_selectXname_language_creatorsX_byXnameX_byXlangX_byXinXcreatedX_name_fold_orderXlocalXX",
        reason = "Local traversals may not traverse past the local star-graph on GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest",
        method = "g_V_hasLabelXsoftwareX_asXnameX_asXlanguageX_asXcreatorsX_selectXname_language_creatorsX_byXnameX_byXlangX_byXinXcreatedX_name_fold_orderXlocalXX",
        reason = "Local traversals may not traverse past the local star-graph on GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest",
        method = "g_V_hasLabelXsoftwareX_asXnameX_asXlanguageX_asXcreatorsX_selectXname_language_creatorsX_byXnameX_byXlangX_byXinXcreatedX_name_fold_orderXlocalXX",
        reason = "Local traversals may not traverse past the local star-graph on GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest",
        method = "g_V_untilXout_outX_repeatXin_asXaX_in_asXbXX_selectXa_bX_byXnameX",
        reason = "Local traversals may not traverse past the local star-graph on GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest",
        method = "g_V_untilXout_outX_repeatXin_asXaXX_selectXaX_byXtailXlocalX_nameX",
        reason = "Local traversals may not traverse past the local star-graph on GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.ReadTest",
        method = "*",
        reason = "The io() step is not supported generally by GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.WriteTest",
        method = "*",
        reason = "The io() step is not supported generally by GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatTest",
        method = "g_V_repeatXout_repeatXoutX_timesX1XX_timesX1X_limitX1X_path_by_name",
        reason = "It is not possible to access more than a path element's id on GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatTest",
        method = "g_V_repeatXoutXknowsXX_untilXrepeatXoutXcreatedXX_emitXhasXname_lopXXX_path_byXnameX",
        reason = "It is not possible to access more than a path element's id on GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatTest",
        method = "g_VX3X_repeatXbothX_createdXX_untilXloops_is_40XXemit_repeatXin_knowsXX_emit_loopsXisX1Xdedup_values",
        reason = "Local traversals may not traverse past the local star-graph on GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatTest",
        method = "g_VX6X_repeatXa_bothXcreatedX_simplePathX_emitXrepeatXb_bothXknowsXX_untilXloopsXbX_asXb_whereXloopsXaX_asXbX_hasXname_vadasXX_dedup_name",
        reason = "Local traversals may not traverse past the local star-graph on GraphComputer")
@GraphProvider.Descriptor(computer = TinkerGraphComputer.class)
public class GryoRemoteGraphGroovyTranslatorComputerProvider extends GryoRemoteGraphGroovyTranslatorProvider {

    @Override
    public GraphTraversalSource traversal(final Graph graph) {
        assert graph instanceof RemoteGraph;
        final int state = TestHelper.RANDOM.nextInt(3);
        switch (state) {
            case 0:
                return super.traversal(graph).withComputer();
            case 1:
                return super.traversal(graph).withComputer(Computer.compute(TinkerGraphComputer.class));
            case 2:
                return super.traversal(graph).withComputer(Computer.compute(TinkerGraphComputer.class).workers(1));
            default:
                throw new IllegalStateException("This state should not have occurred: " + state);
        }
    }
}
