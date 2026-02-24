#region License

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#endregion

using System;
using System.Collections.Generic;
using System.Numerics;
using Gremlin.Net.Process.Traversal;
using Gremlin.Net.Process.Traversal.Strategy.Decoration;
using Gremlin.Net.Process.Traversal.Strategy.Verification;
using Gremlin.Net.Structure;
using Xunit;

namespace Gremlin.Net.UnitTest.Process.Traversal
{
    public class GremlinLangTests : IDisposable
    {
        private readonly GraphTraversalSource _g;

        public GremlinLangTests()
        {
            GremlinLang.ResetCounter();
            _g = new GraphTraversalSource();
        }

        public void Dispose()
        {
        }

        // --- Core Traversal Tests 1-30 ---

        [Fact]
        public void g_V()
        {
            Assert.Equal("g.V()", _g.V().GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_V_1234()
        {
            Assert.Equal("g.V(\"1\",\"2\",\"3\",\"4\")", _g.V("1", "2", "3", "4").GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_V_3_ValueMap_true()
        {
            Assert.Equal("g.V(\"3\").valueMap(true)", _g.V("3").ValueMap<object, object>(true).GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_V_Constant_5()
        {
            Assert.Equal("g.V().constant(5)", _g.V().Constant(5).GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_V_Constant_1_5()
        {
            Assert.Equal("g.V().constant(1.5D)", _g.V().Constant(1.5).GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_V_Constant_Hello()
        {
            Assert.Equal("g.V().constant(\"Hello\")", _g.V().Constant("Hello").GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_V_HasLabel_airport_Limit_5()
        {
            Assert.Equal("g.V().hasLabel(\"airport\").limit(5L)", _g.V().HasLabel("airport").Limit<object>(5).GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_V_HasLabel_P_Within()
        {
            Assert.Equal("g.V().hasLabel(within([\"a\",\"b\",\"c\"]))", _g.V().HasLabel(P.Within("a", "b", "c")).GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_V_HasLabel_airport_continent_Out_Limit_5()
        {
            Assert.Equal("g.V().hasLabel(\"airport\",\"continent\").out().limit(5L)",
                _g.V().HasLabel("airport", "continent").Out().Limit<object>(5).GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_V_HasLabel_airport_Out_Values_code_Limit_5()
        {
            Assert.Equal("g.V().hasLabel(\"airport\").out().values(\"code\").limit(5L)",
                _g.V().HasLabel("airport").Out().Values<object>("code").Limit<object>(5).GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_V_3_As_a_Out_route_Limit_10_Where_Eq_a_By_region()
        {
            Assert.Equal("g.V(\"3\").as(\"a\").out(\"route\").limit(10L).where(eq(\"a\")).by(\"region\")",
                _g.V("3").As("a").Out("route").Limit<object>(10).Where(P.Eq("a")).By("region").GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_V_3_Repeat_Out_route_SimplePath_Times_2_Path_By_code()
        {
            Assert.Equal("g.V(\"3\").repeat(__.out(\"route\").simplePath()).times(2).path().by(\"code\")",
                _g.V("3").Repeat(__.Out("route").SimplePath()).Times(2).Path().By("code").GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_V_HasLabel_airport_Out_Has_region_USTX_Values_code_Limit_5()
        {
            Assert.Equal("g.V().hasLabel(\"airport\").out().has(\"region\",\"US-TX\").values(\"code\").limit(5L)",
                _g.V().HasLabel("airport").Out().Has("region", "US-TX").Values<object>("code").Limit<object>(5).GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_V_HasLabel_airport_Union_Values_city_Values_region_Limit_5()
        {
            Assert.Equal("g.V().hasLabel(\"airport\").union(__.values(\"city\"),__.values(\"region\")).limit(5L)",
                _g.V().HasLabel("airport").Union<object>(__.Values<object>("city"), __.Values<object>("region")).Limit<object>(5).GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_V_3_As_a_Out_route_routes()
        {
            Assert.Equal("g.V(\"3\").as(\"a\").out(\"route\",\"routes\")",
                _g.V("3").As("a").Out("route", "routes").GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_V_Where_Values_runways_Is_5()
        {
            Assert.Equal("g.V().where(__.values(\"runways\").is(5))",
                _g.V().Where(__.Values<object>("runways").Is(5)).GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_V_3_Repeat_Out_SimplePath_Until_Has_code_AGR_Path_By_code_Limit_5()
        {
            Assert.Equal("g.V(\"3\").repeat(__.out().simplePath()).until(__.has(\"code\",\"AGR\")).path().by(\"code\").limit(5L)",
                _g.V("3").Repeat(__.Out().SimplePath()).Until(__.Has("code", "AGR")).Path().By("code").Limit<object>(5).GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_V_HasLabel_airport_Order_By_Id()
        {
            Assert.Equal("g.V().hasLabel(\"airport\").order().by(__.id())",
                _g.V().HasLabel("airport").Order().By(__.Id()).GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_V_HasLabel_airport_Order_By_T_Id()
        {
            Assert.Equal("g.V().hasLabel(\"airport\").order().by(T.id)",
                _g.V().HasLabel("airport").Order().By(T.Id).GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_V_HasLabel_airport_Order_By_Id_Desc()
        {
            Assert.Equal("g.V().hasLabel(\"airport\").order().by(__.id(),Order.desc)",
                _g.V().HasLabel("airport").Order().By(__.Id(), Order.Desc).GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_V_HasLabel_airport_Order_By_code_Desc()
        {
            Assert.Equal("g.V().hasLabel(\"airport\").order().by(\"code\",Order.desc)",
                _g.V().HasLabel("airport").Order().By("code", Order.Desc).GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_V_123_Local_Out_Out_Dedup_Fold()
        {
            Assert.Equal("g.V(\"1\",\"2\",\"3\").local(__.out().out().dedup().fold())",
                _g.V("1", "2", "3").Local<object>(__.Out().Out().Dedup().Fold()).GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_V_3_Out_Path_Count_Scope_Local()
        {
            Assert.Equal("g.V(\"3\").out().path().count(Scope.local)",
                _g.V("3").Out().Path().Count(Scope.Local).GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_E_Count()
        {
            Assert.Equal("g.E().count()", _g.E().Count().GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_V_5_OutE_route_InV_Path_Limit_10()
        {
            Assert.Equal("g.V(\"5\").outE(\"route\").inV().path().limit(10L)",
                _g.V("5").OutE("route").InV().Path().Limit<object>(10).GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_V_5_PropertyMap_Select_Column_Keys()
        {
            Assert.Equal("g.V(\"5\").propertyMap().select(Column.keys)",
                _g.V("5").PropertyMap<object>().Select<object>(Column.Keys).GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_V_5_PropertyMap_Select_Column_Values()
        {
            Assert.Equal("g.V(\"5\").propertyMap().select(Column.values)",
                _g.V("5").PropertyMap<object>().Select<object>(Column.Values).GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_V_3_Values_runways_Math()
        {
            Assert.Equal("g.V(\"3\").values(\"runways\").math(\"_ + 1\")",
                _g.V("3").Values<object>("runways").Math("_ + 1").GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_V_3_Emit_Repeat_Out_SimplePath_Times_3_Limit_5_Path()
        {
            Assert.Equal("g.V(\"3\").emit().repeat(__.out().simplePath()).times(3).limit(5L).path()",
                _g.V("3").Emit().Repeat(__.Out().SimplePath()).Times(3).Limit<object>(5).Path().GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_V_Match_As_a_Has_code_LHR_As_b_Select_b_By_code()
        {
            Assert.Equal("g.V().match(__.as(\"a\").has(\"code\",\"LHR\").as(\"b\")).select(\"b\").by(\"code\")",
                _g.V().Match<object>(__.As("a").Has("code", "LHR").As("b")).Select<object>("b").By("code").GremlinLang.GetGremlin());
        }

        // --- Core Traversal Tests 31-60 ---

        [Fact]
        public void g_V_Has_keyword_property_repeat()
        {
            Assert.Equal("g.V().has(\"test-using-keyword-as-property\",\"repeat\")",
                _g.V().Has("test-using-keyword-as-property", "repeat").GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_V_1_AddE_test_To_V_4()
        {
            Assert.Equal("g.V(\"1\").addE(\"test\").to(__.V(\"4\"))",
                _g.V("1").AddE("test").To(__.V("4")).GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_V_Values_runways_Max()
        {
            Assert.Equal("g.V().values(\"runways\").max()",
                _g.V().Values<object>("runways").Max<object>().GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_V_Values_runways_Min()
        {
            Assert.Equal("g.V().values(\"runways\").min()",
                _g.V().Values<object>("runways").Min<object>().GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_V_Values_runways_Sum()
        {
            Assert.Equal("g.V().values(\"runways\").sum()",
                _g.V().Values<object>("runways").Sum<object>().GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_V_Values_runways_Mean()
        {
            Assert.Equal("g.V().values(\"runways\").mean()",
                _g.V().Values<object>("runways").Mean<object>().GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_WithSack_0_V_35_Sack_Operator_Sum_By_runways_Sack()
        {
            Assert.Equal("g.withSack(0).V(\"3\",\"5\").sack(Operator.sum).by(\"runways\").sack()",
                _g.WithSack(0).V("3", "5").Sack(Operator.Sum).By("runways").Sack<object>().GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_Inject_3_4_5()
        {
            Assert.Equal("g.inject(3,4,5)", _g.Inject(3, 4, 5).GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_Inject_List_3_4_5()
        {
            Assert.Equal("g.inject([3,4,5])",
                _g.Inject(new List<object> { 3, 4, 5 }).GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_Inject_3_4_5_Count()
        {
            Assert.Equal("g.inject(3,4,5).count()", _g.Inject(3, 4, 5).Count().GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_V_Has_runways_Gt_5_Count()
        {
            Assert.Equal("g.V().has(\"runways\",gt(5)).count()",
                _g.V().Has("runways", P.Gt(5)).Count().GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_V_Has_runways_Lte_5_3_Count()
        {
            Assert.Equal("g.V().has(\"runways\",lte(5.3D)).count()",
                _g.V().Has("runways", P.Lte(5.3)).Count().GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_V_Has_code_Within_123_124()
        {
            Assert.Equal("g.V().has(\"code\",within([123,124]))",
                _g.V().Has("code", P.Within(123, 124)).GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_V_Has_code_Within_123_abc()
        {
            Assert.Equal("g.V().has(\"code\",within([123,\"abc\"]))",
                _g.V().Has("code", P.Within(123, "abc")).GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_V_Has_code_Within_abc_123()
        {
            Assert.Equal("g.V().has(\"code\",within([\"abc\",123]))",
                _g.V().Has("code", P.Within("abc", 123)).GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_V_Has_code_Within_abc_xyz()
        {
            Assert.Equal("g.V().has(\"code\",within([\"abc\",\"xyz\"]))",
                _g.V().Has("code", P.Within("abc", "xyz")).GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_V_12_Has_region_Within_USTX_USGA()
        {
            Assert.Equal("g.V(\"1\",\"2\").has(\"region\",within([\"US-TX\",\"US-GA\"]))",
                _g.V("1", "2").Has("region", P.Within("US-TX", "US-GA")).GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_V_And_Has_runways_Gt_5_Has_region_USTX()
        {
            Assert.Equal("g.V().and(__.has(\"runways\",gt(5)),__.has(\"region\",\"US-TX\"))",
                _g.V().And(__.Has("runways", P.Gt(5)), __.Has("region", "US-TX")).GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_V_Union_Has_runways_Gt_5_Has_region_USTX()
        {
            Assert.Equal("g.V().union(__.has(\"runways\",gt(5)),__.has(\"region\",\"US-TX\"))",
                _g.V().Union<object>(__.Has("runways", P.Gt(5)), __.Has("region", "US-TX")).GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_V_3_Choose_Values_runways_Is_3_Constant_three_Constant_not_three()
        {
            Assert.Equal("g.V(\"3\").choose(__.values(\"runways\").is(3),__.constant(\"three\"),__.constant(\"not three\"))",
                _g.V("3").Choose<object>(__.Values<object>("runways").Is(3), __.Constant("three"), __.Constant("not three")).GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_V_3_Choose_Values_runways_Option_1_Option_2()
        {
            Assert.Equal("g.V(\"3\").choose(__.values(\"runways\")).option(1,__.constant(\"three\")).option(2,__.constant(\"not three\"))",
                _g.V("3").Choose<object>(__.Values<object>("runways")).Option(1, __.Constant("three")).Option(2, __.Constant("not three")).GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_V_3_Choose_Values_runways_Option_1_5_Option_2()
        {
            Assert.Equal("g.V(\"3\").choose(__.values(\"runways\")).option(1.5D,__.constant(\"one and a half\")).option(2,__.constant(\"not three\"))",
                _g.V("3").Choose<object>(__.Values<object>("runways")).Option(1.5, __.Constant("one and a half")).Option(2, __.Constant("not three")).GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_V_3_Repeat_Out_SimplePath_Until_Loops_Is_1_Count()
        {
            Assert.Equal("g.V(\"3\").repeat(__.out().simplePath()).until(__.loops().is(1)).count()",
                _g.V("3").Repeat(__.Out().SimplePath()).Until(__.Loops().Is(1)).Count().GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_V_HasLabel_airport_Limit_20_Group_By_region_By_code_Order_Local_By_Keys()
        {
            Assert.Equal("g.V().hasLabel(\"airport\").limit(20L).group().by(\"region\").by(\"code\").order(Scope.local).by(Column.keys)",
                _g.V().HasLabel("airport").Limit<object>(20).Group<object, object>().By("region").By("code").Order(Scope.Local).By(Column.Keys).GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_V_1_As_a_V_2_As_a_Select_Pop_All_a()
        {
            Assert.Equal("g.V(\"1\").as(\"a\").V(\"2\").as(\"a\").select(Pop.all,\"a\")",
                _g.V("1").As("a").V("2").As("a").Select<object>(Pop.All, "a").GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_AddV_test_Property_Cardinality_Set_p1_10()
        {
            Assert.Equal("g.addV(\"test\").property(Cardinality.set,\"p1\",10)",
                _g.AddV("test").Property(Cardinality.Set, "p1", 10).GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_AddV_test_Property_Cardinality_List_p1_10()
        {
            Assert.Equal("g.addV(\"test\").property(Cardinality.list,\"p1\",10)",
                _g.AddV("test").Property(Cardinality.List, "p1", 10).GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_AddV_test_Property_Cardinality_Single_p1_10()
        {
            Assert.Equal("g.addV(\"test\").property(Cardinality.single,\"p1\",10)",
                _g.AddV("test").Property(Cardinality.Single, "p1", 10).GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_V_Limit_5_Order_By_Label()
        {
            Assert.Equal("g.V().limit(5L).order().by(__.label())",
                _g.V().Limit<object>(5).Order().By(__.Label()).GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_V_Range_1_5()
        {
            Assert.Equal("g.V().range(1L,5L)", _g.V().Range<object>(1, 5).GremlinLang.GetGremlin());
        }

        // --- Core Traversal Tests 61-88 ---

        [Fact]
        public void g_AddV_test_Property_p1_123()
        {
            Assert.Equal("g.addV(\"test\").property(\"p1\",123)",
                _g.AddV("test").Property("p1", 123).GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_AddV_test_Property_date_DateTimeOffset()
        {
            Assert.Equal("g.addV(\"test\").property(\"date\",datetime(\"2021-02-01T09:30:00Z\"))",
                _g.AddV("test").Property("date", new DateTimeOffset(2021, 2, 1, 9, 30, 0, TimeSpan.Zero)).GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_AddV_test_Property_date_DateTimeOffset_midnight()
        {
            Assert.Equal("g.addV(\"test\").property(\"date\",datetime(\"2021-02-01T00:00:00Z\"))",
                _g.AddV("test").Property("date", new DateTimeOffset(2021, 2, 1, 0, 0, 0, TimeSpan.Zero)).GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_AddE_route_From_V_1_To_V_2()
        {
            Assert.Equal("g.addE(\"route\").from(__.V(\"1\")).to(__.V(\"2\"))",
                _g.AddE("route").From(__.V("1")).To(__.V("2")).GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_WithSideEffect_a_list_V_3_Select_a()
        {
            Assert.Equal("g.withSideEffect(\"a\",[1,2]).V(\"3\").select(\"a\")",
                _g.WithSideEffect("a", new List<object> { 1, 2 }).V("3").Select<object>("a").GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_WithSideEffect_a_1_V_3_Select_a()
        {
            Assert.Equal("g.withSideEffect(\"a\",1).V(\"3\").select(\"a\")",
                _g.WithSideEffect("a", 1).V("3").Select<object>("a").GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_WithSideEffect_a_abc_V_3_Select_a()
        {
            Assert.Equal("g.withSideEffect(\"a\",\"abc\").V(\"3\").select(\"a\")",
                _g.WithSideEffect("a", "abc").V("3").Select<object>("a").GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_V_Has_airport_region_USNM_Limit_3_Values_elev_Fold_Index()
        {
            Assert.Equal("g.V().has(\"airport\",\"region\",\"US-NM\").limit(3L).values(\"elev\").fold().index()",
                _g.V().Has("airport", "region", "US-NM").Limit<object>(3).Values<object>("elev").Fold().Index<object>().GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_V_3_Repeat_TimeLimit_1000_Out_SimplePath_Until_Has_code_AGR_Path()
        {
            Assert.Equal("g.V(\"3\").repeat(__.timeLimit(1000L).out().simplePath()).until(__.has(\"code\",\"AGR\")).path()",
                _g.V("3").Repeat(__.TimeLimit(1000).Out().SimplePath()).Until(__.Has("code", "AGR")).Path().GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_V_HasLabel_airport_Where_Values_elev_Is_Gt_14000()
        {
            Assert.Equal("g.V().hasLabel(\"airport\").where(__.values(\"elev\").is(gt(14000)))",
                _g.V().HasLabel("airport").Where(__.Values<object>("elev").Is(P.Gt(14000))).GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_V_HasLabel_airport_Where_Out_Count_Is_Gt_250_Values_code()
        {
            Assert.Equal("g.V().hasLabel(\"airport\").where(__.out().count().is(gt(250))).values(\"code\")",
                _g.V().HasLabel("airport").Where(__.Out().Count().Is(P.Gt(250))).Values<object>("code").GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_V_HasLabel_airport_Filter_Out_Count_Is_Gt_250_Values_code()
        {
            Assert.Equal("g.V().hasLabel(\"airport\").filter(__.out().count().is(gt(250))).values(\"code\")",
                _g.V().HasLabel("airport").Filter(__.Out().Count().Is(P.Gt(250))).Values<object>("code").GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_Complex_sack_repeat_until_local_union()
        {
            Assert.Equal("g.withSack(0).V(\"3\").repeat(__.outE(\"route\").sack(Operator.sum).by(\"dist\").inV()).until(__.has(\"code\",\"AGR\").or().loops().is(4)).has(\"code\",\"AGR\").local(__.union(__.path().by(\"code\").by(\"dist\"),__.sack()).fold()).limit(10L)",
                _g.WithSack(0).V("3")
                    .Repeat(__.OutE("route").Sack(Operator.Sum).By("dist").InV())
                    .Until(__.Has("code", "AGR").Or().Loops().Is(4))
                    .Has("code", "AGR")
                    .Local<object>(__.Union<object>(__.Path().By("code").By("dist"), __.Sack<object>()).Fold())
                    .Limit<object>(10).GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_AddV_As_a_AddV_As_b_AddE_knows_From_a_To_b()
        {
            Assert.Equal("g.addV().as(\"a\").addV().as(\"b\").addE(\"knows\").from(\"a\").to(\"b\")",
                _g.AddV().As("a").AddV().As("b").AddE("knows").From("a").To("b").GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_AddV_Person_As_a_AddV_Person_As_b_AddE_knows_From_a_To_b()
        {
            Assert.Equal("g.addV(\"Person\").as(\"a\").addV(\"Person\").as(\"b\").addE(\"knows\").from(\"a\").to(\"b\")",
                _g.AddV("Person").As("a").AddV("Person").As("b").AddE("knows").From("a").To("b").GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_V_3_Project_Out_In_By_Out_Count_By_In_Count()
        {
            Assert.Equal("g.V(\"3\").project(\"Out\",\"In\").by(__.out().count()).by(__.in().count())",
                _g.V("3").Project<object>("Out", "In").By(__.Out().Count()).By(__.In().Count()).GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_V_44_Out_Aggregate_a_Out_Where_Within_a_Path()
        {
            Assert.Equal("g.V(\"44\").out().aggregate(\"a\").out().where(within([\"a\"])).path()",
                _g.V("44").Out().Aggregate("a").Out().Where(P.Within("a")).Path().GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_V_Has_date_DateTimeOffset()
        {
            Assert.Equal("g.V().has(\"date\",datetime(\"2021-02-22T00:00:00Z\"))",
                _g.V().Has("date", new DateTimeOffset(2021, 2, 22, 0, 0, 0, TimeSpan.Zero)).GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_V_Has_date_Within_dates()
        {
            Assert.Equal("g.V().has(\"date\",within([datetime(\"2021-02-22T00:00:00Z\"),datetime(\"2021-01-01T00:00:00Z\")]))",
                _g.V().Has("date", P.Within(
                    new DateTimeOffset(2021, 2, 22, 0, 0, 0, TimeSpan.Zero),
                    new DateTimeOffset(2021, 1, 1, 0, 0, 0, TimeSpan.Zero))).GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_V_Has_date_Between_dates()
        {
            Assert.Equal("g.V().has(\"date\",between(datetime(\"2021-01-01T00:00:00Z\"),datetime(\"2021-02-22T00:00:00Z\")))",
                _g.V().Has("date", P.Between(
                    new DateTimeOffset(2021, 1, 1, 0, 0, 0, TimeSpan.Zero),
                    new DateTimeOffset(2021, 2, 22, 0, 0, 0, TimeSpan.Zero))).GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_V_Has_date_Inside_dates()
        {
            Assert.Equal("g.V().has(\"date\",inside(datetime(\"2021-01-01T00:00:00Z\"),datetime(\"2021-02-22T00:00:00Z\")))",
                _g.V().Has("date", P.Inside(
                    new DateTimeOffset(2021, 1, 1, 0, 0, 0, TimeSpan.Zero),
                    new DateTimeOffset(2021, 2, 22, 0, 0, 0, TimeSpan.Zero))).GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_V_Has_date_Gt_DateTimeOffset()
        {
            Assert.Equal("g.V().has(\"date\",gt(datetime(\"2021-01-01T09:30:00Z\")))",
                _g.V().Has("date", P.Gt(new DateTimeOffset(2021, 1, 1, 9, 30, 0, TimeSpan.Zero))).GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_V_Has_runways_Between_3_5()
        {
            Assert.Equal("g.V().has(\"runways\",between(3,5))",
                _g.V().Has("runways", P.Between(3, 5)).GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_V_Has_runways_Inside_3_5()
        {
            Assert.Equal("g.V().has(\"runways\",inside(3,5))",
                _g.V().Has("runways", P.Inside(3, 5)).GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_V_44_OutE_ElementMap()
        {
            Assert.Equal("g.V(\"44\").outE().elementMap()",
                _g.V("44").OutE().ElementMap<object>().GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_V_44_ValueMap_By_Unfold()
        {
            Assert.Equal("g.V(\"44\").valueMap().by(__.unfold())",
                _g.V("44").ValueMap<object, object>().By(__.Unfold<object>()).GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_V_44_ValueMap_With_Tokens_Labels()
        {
            Assert.Equal("g.V(\"44\").valueMap().with(\"~tinkerpop.valueMap.tokens\",2)",
                _g.V("44").ValueMap<object, object>().With(WithOptions.Tokens, WithOptions.Labels).GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_V_44_ValueMap_With_Tokens()
        {
            Assert.Equal("g.V(\"44\").valueMap().with(\"~tinkerpop.valueMap.tokens\")",
                _g.V("44").ValueMap<object, object>().With(WithOptions.Tokens).GremlinLang.GetGremlin());
        }

        // --- Strategy Tests 89-97 ---

        [Fact]
        public void g_WithStrategies_ReadOnlyStrategy_AddV_test()
        {
            Assert.Equal("g.withStrategies(ReadOnlyStrategy).addV(\"test\")",
                _g.WithStrategies(new ReadOnlyStrategy()).AddV("test").GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_WithStrategies_SubgraphStrategy_vertices_edges_V_Count()
        {
            var result = _g.WithStrategies(new SubgraphStrategy(
                vertices: __.Has("region", "US-TX"),
                edges: __.HasLabel("route"))).V().Count().GremlinLang.GetGremlin();
            Assert.Contains("g.withStrategies(new SubgraphStrategy(", result);
            Assert.Contains("vertices:__.has(\"region\",\"US-TX\")", result);
            Assert.Contains("edges:__.hasLabel(\"route\")", result);
            Assert.Contains(")).V().count()", result);
        }

        [Fact]
        public void g_WithStrategies_SubgraphStrategy_vertexProperties_V_Count()
        {
            Assert.Equal("g.withStrategies(new SubgraphStrategy(vertexProperties:__.hasNot(\"runways\"))).V().count()",
                _g.WithStrategies(new SubgraphStrategy(vertexProperties: __.HasNot("runways"))).V().Count().GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_WithStrategies_SubgraphStrategy_vertices_vertexProperties_V_Count()
        {
            var result = _g.WithStrategies(new SubgraphStrategy(
                vertices: __.Has("region", "US-TX"),
                vertexProperties: __.HasNot("runways"))).V().Count().GremlinLang.GetGremlin();
            Assert.Contains("g.withStrategies(new SubgraphStrategy(", result);
            Assert.Contains("vertices:__.has(\"region\",\"US-TX\")", result);
            Assert.Contains("vertexProperties:__.hasNot(\"runways\")", result);
            Assert.Contains(")).V().count()", result);
        }

        [Fact]
        public void g_WithStrategies_ReadOnly_SubgraphStrategy_vertices_edges_V_Count()
        {
            var result = _g.WithStrategies(new ReadOnlyStrategy(), new SubgraphStrategy(
                vertices: __.Has("region", "US-TX"),
                edges: __.HasLabel("route"))).V().Count().GremlinLang.GetGremlin();
            Assert.Contains("ReadOnlyStrategy", result);
            Assert.Contains("new SubgraphStrategy(", result);
            Assert.Contains("vertices:__.has(\"region\",\"US-TX\")", result);
            Assert.Contains("edges:__.hasLabel(\"route\")", result);
        }

        [Fact]
        public void g_WithStrategies_ReadOnly_SubgraphStrategy_vertices_V_Count()
        {
            Assert.Equal("g.withStrategies(ReadOnlyStrategy,new SubgraphStrategy(vertices:__.has(\"region\",\"US-TX\"))).V().count()",
                _g.WithStrategies(new ReadOnlyStrategy(), new SubgraphStrategy(
                    vertices: __.Has("region", "US-TX"))).V().Count().GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_WithStrategies_OptionsStrategy_V_Count()
        {
            // OptionsStrategy is extracted, not rendered in the script
            Assert.Equal("g.V().count()",
                _g.WithStrategies(new OptionsStrategy(new Dictionary<string, object> { { "evaluationTimeout", 500 } })).V().Count().GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_WithStrategies_PartitionStrategy_AddV_test()
        {
            var result = _g.WithStrategies(new PartitionStrategy(
                partitionKey: "partition",
                writePartition: "a",
                readPartitions: new List<string> { "a" },
                includeMetaProperties: false)).AddV("test").GremlinLang.GetGremlin();
            Assert.Contains("g.withStrategies(new PartitionStrategy(", result);
            Assert.Contains("partitionKey:\"partition\"", result);
            Assert.Contains("writePartition:\"a\"", result);
            Assert.Contains("readPartitions:[\"a\"]", result);
            Assert.Contains(")).addV(\"test\")", result);
        }

        [Fact]
        public void g_WithStrategies_VertexProgramStrategy_V_ShortestPath_With()
        {
            Assert.Equal("g.withStrategies(VertexProgramStrategy).V().shortestPath().with(\"~tinkerpop.shortestPath.target\",__.has(\"name\",\"peter\"))",
                _g.WithStrategies(new VertexProgramStrategy()).V().ShortestPath().With("~tinkerpop.shortestPath.target", __.Has("name", "peter")).GremlinLang.GetGremlin());
        }

        // --- TextP and Misc Tests 98-105 ---

        [Fact]
        public void g_V_Coalesce_E_AddE_person()
        {
            Assert.Equal("g.V().coalesce(__.E(),__.addE(\"person\"))",
                _g.V().Coalesce<object>(__.E(), __.AddE("person")).GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_Inject_1_E()
        {
            Assert.Equal("g.inject(1).E()", _g.Inject(1).E().GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_V_Has_p1_StartingWith_foo()
        {
            Assert.Equal("g.V().has(\"p1\",startingWith(\"foo\"))",
                _g.V().Has("p1", TextP.StartingWith("foo")).GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_V_Has_p1_EndingWith_foo()
        {
            Assert.Equal("g.V().has(\"p1\",endingWith(\"foo\"))",
                _g.V().Has("p1", TextP.EndingWith("foo")).GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_V_Has_p1_Containing_foo()
        {
            Assert.Equal("g.V().has(\"p1\",containing(\"foo\"))",
                _g.V().Has("p1", TextP.Containing("foo")).GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_V_Has_p1_null()
        {
            Assert.Equal("g.V().has(\"p1\",null)",
                _g.V().Has("p1", (object?)null).GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_V_Vertex_0_person()
        {
            Assert.Equal("g.V(0)", _g.V(new Vertex(0, "person")).GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_Inject_Guid()
        {
            var guid = new Guid("f47af10b-58cc-4372-a567-0f02b2f3d479");
            Assert.Equal("g.inject(UUID(\"f47af10b-58cc-4372-a567-0f02b2f3d479\"))",
                _g.Inject((object)guid).GremlinLang.GetGremlin());
        }

        // --- Python-Only Tests 106-116 ---

        [Fact]
        public void g_E_Edge_renders_as_parameter()
        {
            GremlinLang.ResetCounter();
            var edge = new Edge(2, new Vertex(1), "knows", new Vertex(3));
            var result = _g.E(edge).GremlinLang.GetGremlin();
            Assert.Equal("g.E(_0)", result);
        }

        [Fact]
        public void g_Inject_VertexProperty_renders_as_parameter()
        {
            GremlinLang.ResetCounter();
            var vp = new VertexProperty(3, "time", "18:00", null);
            var result = _g.Inject((object)vp).GremlinLang.GetGremlin();
            Assert.Equal("g.inject(_0)", result);
        }

        [Fact]
        public void g_Inject_dicts_with_enum_keys()
        {
            GremlinLang.ResetCounter();
            var dict1 = new Dictionary<object, object> { { "name", "java" } };
            var dict2 = new Dictionary<object, object> { { T.Id, 0 } };
            var dict3 = new Dictionary<object, object>();
            var dict4 = new Dictionary<object, object>
            {
                { "age", 10.0 },
                { "pos_inf", double.PositiveInfinity },
                { "neg_inf", double.NegativeInfinity },
                { "nan", double.NaN }
            };
            var result = _g.Inject((object)dict1, (object)dict2, (object)dict3, (object)dict4).GremlinLang.GetGremlin();
            Assert.Contains("[\"name\":\"java\"]", result);
            Assert.Contains("[(T.id):0]", result);
            Assert.Contains("[:]", result);
            Assert.Contains("10.0D", result);
            Assert.Contains("+Infinity", result);
            Assert.Contains("-Infinity", result);
            Assert.Contains("NaN", result);
        }

        [Fact]
        public void g_Inject_1_0_Is_Eq_1_Or_Gt_2_Or_Lte_3_Or_Gte_4()
        {
            Assert.Equal("g.inject(1.0D).is(eq(1).or(gt(2)).or(lte(3)).or(gte(4)))",
                _g.Inject(1.0).Is(P.Eq(1).Or(P.Gt(2)).Or(P.Lte(3)).Or(P.Gte(4))).GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_V_HasLabel_person_Has_age_complex_connective_P()
        {
            Assert.Equal("g.V().hasLabel(\"person\").has(\"age\",gt(10).or(gte(11).and(lt(20))).and(lt(29).or(eq(35)))).values(\"name\")",
                _g.V().HasLabel("person").Has("age",
                    P.Gt(10).Or(P.Gte(11).And(P.Lt(20))).And(P.Lt(29).Or(P.Eq(35))))
                    .Values<object>("name").GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_Inject_HashSet()
        {
            Assert.Equal("g.inject({\"a\"})",
                _g.Inject((object)new HashSet<object> { "a" }).GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_MergeV_null()
        {
            Assert.Equal("g.mergeV(null)", _g.MergeV((IDictionary<object, object>?)null).GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_MergeE_null()
        {
            Assert.Equal("g.mergeE(null)", _g.MergeE((IDictionary<object, object?>?)null).GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_AddV_escaped_quotes()
        {
            Assert.Equal("g.addV(\"\\\"test\\\"\")", _g.AddV("\"test\"").GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_AddV_escaped_mixed_quotes()
        {
            Assert.Equal("g.addV(\"t'\\\"est\")", _g.AddV("t'\"est").GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_Inject_all_numeric_types()
        {
            Assert.Equal("g.inject(true,1B,2S,3,4L,5.0D,6.0D,7N,8M)",
                _g.Inject((object)true, (object)(byte)1, (object)(short)2, (object)3, (object)(long)4,
                    (object)5.0, (object)6.0, (object)new BigInteger(7), (object)(decimal)8).GremlinLang.GetGremlin());
        }

        // --- Go-Only Tests 117-123 ---

        [Fact]
        public void g_V_44_ValueMap_With_All()
        {
            Assert.Equal("g.V(\"44\").valueMap().with(\"~tinkerpop.valueMap.tokens\",15)",
                _g.V("44").ValueMap<object, object>().With(WithOptions.Tokens, WithOptions.All).GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_V_44_ValueMap_With_Indexer()
        {
            Assert.Equal("g.V(\"44\").valueMap().with(\"~tinkerpop.index.indexer\")",
                _g.V("44").ValueMap<object, object>().With(WithOptions.Indexer).GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_V_Filter_Has_name_TextP_StartingWith_m_Or_StartingWith_p()
        {
            Assert.Equal("g.V().filter(__.has(\"name\",startingWith(\"m\").or(startingWith(\"p\"))))",
                _g.V().Filter(__.Has("name", TextP.StartingWith("m").Or(TextP.StartingWith("p")))).GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_V_Has_person_age_Within_empty()
        {
            Assert.Equal("g.V().has(\"person\",\"age\",within())",
                _g.V().Has("person", "age", P.Within()).GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_Inject_NaN_Is_Eq_NaN()
        {
            Assert.Equal("g.inject(NaN).is(eq(NaN))",
                _g.Inject(double.NaN).Is(P.Eq(double.NaN)).GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_V_HasLabel_person_Values_age_Is_Between_26_30_Or_Gt_34()
        {
            Assert.Equal("g.V().hasLabel(\"person\").values(\"age\").is(between(26,30).or(gt(34)))",
                _g.V().HasLabel("person").Values<object>("age").Is(P.Between(26, 30).Or(P.Gt(34))).GremlinLang.GetGremlin());
        }

        [Fact]
        public void g_Inject_1_AsNumber_GType_Double()
        {
            Assert.Equal("g.inject(1).asNumber(GType.DOUBLE)",
                _g.Inject(1).AsNumber(GType.Double).GremlinLang.GetGremlin());
        }

        // --- GValue Validation Tests 124-130 ---

        [Fact]
        public void GValue_null_name_throws_ArgumentNullException()
        {
            Assert.Throws<ArgumentNullException>(() => new GValue<int>(null!, 1));
        }

        [Fact]
        public void GValue_special_char_name_throws_ArgumentException()
        {
            Assert.Throws<ArgumentException>(() => new GValue<int>("\"", 1));
        }

        [Fact]
        public void GValue_numeric_name_throws_ArgumentException()
        {
            Assert.Throws<ArgumentException>(() => new GValue<int>("1", 1));
        }

        [Fact]
        public void GValue_invalid_identifier_name_throws_ArgumentException()
        {
            Assert.Throws<ArgumentException>(() => new GValue<int>("1a", 1));
        }

        [Fact]
        public void GValue_underscore_name_throws_ArgumentException()
        {
            Assert.Throws<ArgumentException>(() => new GValue<int>("_1", 1));
        }

        [Fact]
        public void GValue_duplicate_name_different_value_throws_ArgumentException()
        {
            var gval1 = new GValue<int>("x", 1);
            var gval2 = new GValue<int>("x", 2);
            Assert.Throws<ArgumentException>(() => _g.Inject((object)gval1).V(gval2));
        }

        [Fact]
        public void GValue_reuse_same_instance()
        {
            var gval = new GValue<IList<object>>("ids", new List<object> { 1, 2, 3 });
            var result = _g.Inject((object)gval).V(gval).GremlinLang;
            Assert.Equal("g.inject(ids).V(ids)", result.GetGremlin());
            Assert.True(result.Parameters.ContainsKey("ids"));
        }
    }
}
