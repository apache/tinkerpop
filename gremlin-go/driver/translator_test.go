/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
*/

package gremlingo

import (
	"testing"
)

func Test_translator_Translate(t *testing.T) {
	tests := []struct {
		assert       (func(g *GraphTraversalSource) *GraphTraversal)
		expect       string
		customSource *string
	}{
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V("1")
			},
			expect: "g.V(\"1\")",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V("1", "2", "3", "4")
			},
			expect: "g.V(\"1\", \"2\", \"3\", \"4\")",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V("3").ValueMap(true)
			},
			expect: "g.V(\"3\").valueMap(true)",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V().Constant(5)
			},
			expect: "g.V().constant(5)",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V().Constant(1.5)
			},
			expect: "g.V().constant(1.5)",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V().Constant("Hello")
			},
			expect: "g.V().constant(\"Hello\")",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V().HasLabel("airport").Limit(5)
			},
			expect: "g.V().hasLabel(\"airport\").limit(5)",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V().HasLabel(P.Within("a", "b", "c"))
			},
			expect: "g.V().hasLabel(within(\"a\", \"b\", \"c\"))",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V().HasLabel("airport", "continent").Out().Limit(5)
			},
			expect: "g.V().hasLabel(\"airport\", \"continent\").out().limit(5)",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V().HasLabel("airport").Out().Values("code").Limit(5)
			},
			expect: "g.V().hasLabel(\"airport\").out().values(\"code\").limit(5)",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V("3").As("a").Out("route").Limit(10).Where(P.Eq("a")).By("region")
			},
			expect: "g.V(\"3\").as(\"a\").out(\"route\").limit(10).where(eq(\"a\")).by(\"region\")",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V("3").Repeat(T__.Out("route").SimplePath()).Times(2).Path().By("code")
			},
			expect: "g.V(\"3\").repeat(__.out(\"route\").simplePath()).times(2).path().by(\"code\")",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V().HasLabel("airport").Out().Has("region", "US-TX").Values("code").Limit(5)
			},
			expect: "g.V().hasLabel(\"airport\").out().has(\"region\", \"US-TX\").values(\"code\").limit(5)",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V().HasLabel("airport").Union(T__.Values("city"), T__.Values("region")).Limit(5)
			},
			expect: "g.V().hasLabel(\"airport\").union(__.values(\"city\"), __.values(\"region\")).limit(5)",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V("3").As("a").Out("route", "routes")
			},
			expect: "g.V(\"3\").as(\"a\").out(\"route\", \"routes\")",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V().Where(T__.Values("runways").Is(5))
			},
			expect: "g.V().where(__.values(\"runways\").is(5))",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V("3").Repeat(T__.Out().SimplePath()).Until(T__.Has("code", "AGR")).Path().By("code").Limit(5)
			},
			expect: "g.V(\"3\").repeat(__.out().simplePath()).until(__.has(\"code\", \"AGR\")).path().by(\"code\").limit(5)",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V().HasLabel("airport").Order().By(T__.Id())
			},
			expect: "g.V().hasLabel(\"airport\").order().by(__.id())",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V().HasLabel("airport").Order().By(T__.Id(), Order.Desc)
			},
			expect: "g.V().hasLabel(\"airport\").order().by(__.id(), desc)",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V().HasLabel("airport").Order().By("code", Order.Desc)
			},
			expect: "g.V().hasLabel(\"airport\").order().by(\"code\", desc)",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V("1", "2", "3").Local(T__.Out().Out().Dedup().Fold())
			},
			expect: "g.V(\"1\", \"2\", \"3\").local(__.out().out().dedup().fold())",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V("3").Out().Path().Count(Scope.Local)
			},
			expect: "g.V(\"3\").out().path().count(local)",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.E().Count()
			},
			expect: "g.E().count()",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V("5").OutE("route").InV().Path().Limit(10)
			},
			expect: "g.V(\"5\").outE(\"route\").inV().path().limit(10)",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V("5").PropertyMap().Select(Column.Keys)
			},
			expect: "g.V(\"5\").propertyMap().select(keys)",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V("5").PropertyMap().Select(Column.Values)
			},
			expect: "g.V(\"5\").propertyMap().select(values)",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V("3").Values("runways").Math("_ + 1")
			},
			expect: "g.V(\"3\").values(\"runways\").math(\"_ + 1\")",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V("3").Emit().Repeat(T__.Out().SimplePath()).Times(3).Limit(5).Path()
			},
			expect: "g.V(\"3\").emit().repeat(__.out().simplePath()).times(3).limit(5).path()",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V().Match(T__.As("a").Has("code", "LHR").As("b")).Select("b").By("code")
			},
			expect: "g.V().match(__.as(\"a\").has(\"code\", \"LHR\").as(\"b\")).select(\"b\").by(\"code\")",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V().Has("test-using-keyword-as-property", "repeat")
			},
			expect: "g.V().has(\"test-using-keyword-as-property\", \"repeat\")",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V("1").AddE("test").To(T__.V("4"))
			},
			expect: "g.V(\"1\").addE(\"test\").to(__.V(\"4\"))",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V().Values("runways").Max()
			},
			expect: "g.V().values(\"runways\").max()",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V().Values("runways").Min()
			},
			expect: "g.V().values(\"runways\").min()",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V().Values("runways").Sum()
			},
			expect: "g.V().values(\"runways\").sum()",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V().Values("runways").Mean()
			},
			expect: "g.V().values(\"runways\").mean()",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.WithSack(0).V("3", "5").Sack(Operator.Sum).By("runways").Sack()
			},
			expect:       "g.withSack(0).V(\"3\", \"5\").sack(sum).by(\"runways\").sack()",
			customSource: toPtr("g.withSack(0)"),
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V("3").Values("runways").Store("x").V("4").Values("runways").Store("x").By(T__.Constant(1)).V("6").Store("x").By(T__.Constant(1)).Select("x").Unfold().Sum()
			},
			expect: "g.V(\"3\").values(\"runways\").store(\"x\").V(\"4\").values(\"runways\").store(\"x\").by(__.constant(1)).V(\"6\").store(\"x\").by(__.constant(1)).select(\"x\").unfold().sum()",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.Inject(3, 4, 5)
			},
			expect: "g.inject(3, 4, 5)",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.Inject([]int{3, 4, 5})
			},
			expect: "g.inject([3, 4, 5])",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.Inject(3, 4, 5).Count()
			},
			expect: "g.inject(3, 4, 5).count()",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V().Has("runways", P.Gt(5)).Count()
			},
			expect: "g.V().has(\"runways\", gt(5)).count()",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V().Has("runways", P.Lte(5.3)).Count()
			},
			expect: "g.V().has(\"runways\", lte(5.3)).count()",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V().Has("code", P.Within(123, 124))
			},
			expect: "g.V().has(\"code\", within(123, 124))",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V().Has("code", P.Within(123, "abc"))
			},
			expect: "g.V().has(\"code\", within(123, \"abc\"))",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V().Has("code", P.Within("abc", 123))
			},
			expect: "g.V().has(\"code\", within(\"abc\", 123))",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V().Has("code", P.Within("abc", "xyz"))
			},
			expect: "g.V().has(\"code\", within(\"abc\", \"xyz\"))",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V("1", "2").Has("region", P.Within("US-TX", "US-GA"))
			},
			expect: "g.V(\"1\", \"2\").has(\"region\", within(\"US-TX\", \"US-GA\"))",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V().Has("code", P.Without("LAX", "JFK"))
			},
			expect: "g.V().has(\"code\", without(\"LAX\", \"JFK\"))",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V().Has("code", P.Without(123, "abc"))
			},
			expect: "g.V().has(\"code\", without(123, \"abc\"))",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V().And(T__.Has("runways", P.Gt(5)), T__.Has("region", "US-TX"))
			},
			expect: "g.V().and(__.has(\"runways\", gt(5)), __.has(\"region\", \"US-TX\"))",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V().Union(T__.Has("runways", P.Gt(5)), T__.Has("region", "US-TX"))
			},
			expect: "g.V().union(__.has(\"runways\", gt(5)), __.has(\"region\", \"US-TX\"))",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V("3").Choose(T__.Values("runways").Is(3), T__.Constant("three"), T__.Constant("not three"))
			},
			expect: "g.V(\"3\").choose(__.values(\"runways\").is(3), __.constant(\"three\"), __.constant(\"not three\"))",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V("3").Choose(T__.Values("runways")).Option(1, T__.Constant("three")).Option(2, T__.Constant("not three"))
			},
			expect: "g.V(\"3\").choose(__.values(\"runways\")).option(1, __.constant(\"three\")).option(2, __.constant(\"not three\"))",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V("3").Choose(T__.Values("runways")).Option(1.5, T__.Constant("one and a half")).Option(2, T__.Constant("not three"))
			},
			expect: "g.V(\"3\").choose(__.values(\"runways\")).option(1.5, __.constant(\"one and a half\")).option(2, __.constant(\"not three\"))",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V("3").Repeat(T__.Out().SimplePath()).Until(T__.Loops().Is(1)).Count()
			},
			expect: "g.V(\"3\").repeat(__.out().simplePath()).until(__.loops().is(1)).count()",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V().HasLabel("airport").Limit(20).Group().By("region").By("code").Order(Scope.Local).By(Column.Keys)
			},
			expect: "g.V().hasLabel(\"airport\").limit(20).group().by(\"region\").by(\"code\").order(local).by(keys)",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V("1").As("a").V("2").As("a").Select(Pop.All, "a")
			},
			expect: "g.V(\"1\").as(\"a\").V(\"2\").as(\"a\").select(all, \"a\")",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.AddV("test").Property(Cardinality.Set, "p1", 10)
			},
			expect: "g.addV(\"test\").property(set, \"p1\", 10)",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.AddV("test").Property(Cardinality.List, "p1", 10)
			},
			expect: "g.addV(\"test\").property(list, \"p1\", 10)",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.AddV("test").Property(Cardinality.Single, "p1", 10)
			},
			expect: "g.addV(\"test\").property(single, \"p1\", 10)",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V().Limit(5).Order().By(T__.Label())
			},
			expect: "g.V().limit(5).order().by(__.label())",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V().Range(1, 5)
			},
			expect: "g.V().range(1, 5)",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.AddV("test").Property("p1", 123)
			},
			expect: "g.addV(\"test\").property(\"p1\", 123)",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.AddE("route").From(T__.V("1")).To(T__.V("2"))
			},
			expect: "g.addE(\"route\").from(__.V(\"1\")).to(__.V(\"2\"))",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.WithSideEffect("a", []int{1, 2}).V("3").Select("a")
			},
			expect:       "g.withSideEffect(\"a\", [1, 2]).V(\"3\").select(\"a\")",
			customSource: toPtr("g.withSideEffect(\"a\", [1, 2])"),
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.WithSideEffect("a", 1).V("3").Select("a")
			},
			expect:       "g.withSideEffect(\"a\", 1).V(\"3\").select(\"a\")",
			customSource: toPtr("g.withSideEffect(\"a\", 1)"),
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.WithSideEffect("a", "abc").V("3").Select("a")
			},
			expect:       "g.withSideEffect(\"a\", \"abc\").V(\"3\").select(\"a\")",
			customSource: toPtr("g.withSideEffect(\"a\", \"abc\")"),
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V().Has("airport", "region", "US-NM").Limit(3).Values("elev").Fold().Index()
			},
			expect: "g.V().has(\"airport\", \"region\", \"US-NM\").limit(3).values(\"elev\").fold().index()",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V("3").Repeat(T__.TimeLimit(1000).Out().SimplePath()).Until(T__.Has("code", "AGR")).Path()
			},
			expect: "g.V(\"3\").repeat(__.timeLimit(1000).out().simplePath()).until(__.has(\"code\", \"AGR\")).path()",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V().HasLabel("airport").Where(T__.Values("elev").Is(P.Gt(14000)))
			},
			expect: "g.V().hasLabel(\"airport\").where(__.values(\"elev\").is(gt(14000)))",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V().HasLabel("airport").Where(T__.Out().Count().Is(P.Gt(250))).Values("code")
			},
			expect: "g.V().hasLabel(\"airport\").where(__.out().count().is(gt(250))).values(\"code\")",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V().HasLabel("airport").Filter(T__.Out().Count().Is(P.Gt(250))).Values("code")
			},
			expect: "g.V().hasLabel(\"airport\").filter(__.out().count().is(gt(250))).values(\"code\")",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.WithSack(0).V("3").Repeat(
					T__.OutE("route").Sack(Operator.Sum).By("dist").InV(),
				).Until(T__.Has("code", "AGR").Or().Loops().Is(4)).Has("code", "AGR").Local(
					T__.Union(
						T__.Path().By("code").By("dist"),
						T__.Sack(),
					).Fold(),
				).Limit(10)
			},
			expect:       "g.withSack(0).V(\"3\").repeat(__.outE(\"route\").sack(sum).by(\"dist\").inV()).until(__.has(\"code\", \"AGR\").or().loops().is(4)).has(\"code\", \"AGR\").local(__.union(__.path().by(\"code\").by(\"dist\"), __.sack()).fold()).limit(10)",
			customSource: toPtr("g.withSack(0)"),
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.AddV().As("a").AddV().As("b").AddE("knows").From("a").To("b")
			},
			expect: "g.addV().as(\"a\").addV().as(\"b\").addE(\"knows\").from(\"a\").to(\"b\")",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.AddV("Person").As("a").AddV("Person").As("b").AddE("knows").From("a").To("b")
			},
			expect: "g.addV(\"Person\").as(\"a\").addV(\"Person\").as(\"b\").addE(\"knows\").from(\"a\").to(\"b\")",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V("3").Project("Out", "In").By(T__.Out().Count()).By(T__.In().Count())
			},
			expect: "g.V(\"3\").project(\"Out\", \"In\").by(__.out().count()).by(__.in().count())",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V("44").Out().Aggregate("a").Out().Where(P.Within("a")).Path()
			},
			expect: "g.V(\"44\").out().aggregate(\"a\").out().where(within(\"a\")).path()",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V().Has("runways", P.Between(3, 5))
			},
			expect: "g.V().has(\"runways\", between(3, 5))",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V().Has("runways", P.Inside(3, 5))
			},
			expect: "g.V().has(\"runways\", inside(3, 5))",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V("44").OutE().ElementMap()
			},
			expect: "g.V(\"44\").outE().elementMap()",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V("44").ValueMap().By(T__.Unfold())
			},
			expect: "g.V(\"44\").valueMap().by(__.unfold())",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V("44").ValueMap().With(WithOptions.Tokens, WithOptions.Labels)
			},
			expect: "g.V(\"44\").valueMap().with(\"~tinkerpop.valueMap.tokens\", 2)",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V("44").ValueMap().With(WithOptions.Tokens)
			},
			expect: "g.V(\"44\").valueMap().with(\"~tinkerpop.valueMap.tokens\")",
		},
	}
	for _, tt := range tests {
		t.Run(tt.expect, func(t *testing.T) {
			tr := NewTranslator("g")
			if tt.customSource != nil {
				tr = NewTranslator(*tt.customSource)
			}

			s := NewGraphTraversalSource(&Graph{}, nil, nil)
			g := tt.assert(s)
			if got := tr.Translate(g.Bytecode); got != tt.expect {
				t.Errorf("translator.Translate() = %v, want %v", got, tt.expect)
			}
		})
	}
}

func toPtr[T any](x T) *T {
	return &x
}
