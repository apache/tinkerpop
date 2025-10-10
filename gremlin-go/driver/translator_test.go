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
	"regexp"
	"testing"
	"time"
)

func Test_translator_Translate(t *testing.T) {
	type test struct {
		name                      string
		assert                    func(g *GraphTraversalSource) *GraphTraversal
		equals                    string
		only                      bool
		skip                      bool
		wantErr                   bool
		containsRandomClassParams bool
	}
	tests := []test{
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal { return g.V() },
			equals: "g.V()",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal { return g.V("1", "2", "3", "4") },
			equals: "g.V('1','2','3','4')",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal { return g.V("3").ValueMap(true) },
			equals: "g.V('3').valueMap(true)",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal { return g.V().Constant(5) },
			equals: "g.V().constant(5)",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal { return g.V().Constant(1.5) },
			equals: "g.V().constant(1.5)",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal { return g.V().Constant("Hello") },
			equals: "g.V().constant('Hello')",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal { return g.V().HasLabel("airport").Limit(5) },
			equals: "g.V().hasLabel('airport').limit(5)",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal { return g.V().HasLabel(P.Within("a", "b", "c")) },
			equals: "g.V().hasLabel(within(['a','b','c']))",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V().HasLabel("airport", "continent").Out().Limit(5)
			},
			equals: "g.V().hasLabel('airport','continent').out().limit(5)",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V().HasLabel("airport").Out().Values("code").Limit(5)
			},
			equals: "g.V().hasLabel('airport').out().values('code').limit(5)",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V("3").As("a").Out("route").Limit(10).Where(P.Eq("a")).By("region")
			},
			equals: "g.V('3').as('a').out('route').limit(10).where(eq('a')).by('region')",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V("3").Repeat(T__.Out("route").SimplePath()).Times(2).Path().By("code")
			},
			equals: "g.V('3').repeat(out('route').simplePath()).times(2).path().by('code')",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V().HasLabel("airport").Out().Has("region", "US-TX").Values("code").Limit(5)
			},
			equals: "g.V().hasLabel('airport').out().has('region','US-TX').values('code').limit(5)",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V().HasLabel("airport").Union(T__.Values("city"), T__.Values("region")).Limit(5)
			},
			equals: "g.V().hasLabel('airport').union(values('city'),values('region')).limit(5)",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal { return g.V("3").As("a").Out("route", "routes") },
			equals: "g.V('3').as('a').out('route','routes')",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal { return g.V().Where(T__.Values("runways").Is(5)) },
			equals: "g.V().where(values('runways').is(5))",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V("3").Repeat(T__.Out().SimplePath()).Until(T__.Has("code", "AGR")).Path().By("code").Limit(5)
			},
			equals: "g.V('3').repeat(out().simplePath()).until(has('code','AGR')).path().by('code').limit(5)",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal { return g.V().HasLabel("airport").Order().By(T__.Id()) },
			equals: "g.V().hasLabel('airport').order().by(id())",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal { return g.V().HasLabel("airport").Order().By(T.Id) },
			equals: "g.V().hasLabel('airport').order().by(id)",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V().HasLabel("airport").Order().By(T__.Id(), Order.Desc)
			},
			equals: "g.V().hasLabel('airport').order().by(id(),desc)",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V().HasLabel("airport").Order().By("code", Order.Desc)
			},
			equals: "g.V().hasLabel('airport').order().by('code',desc)",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V("1", "2", "3").Local(T__.Out().Out().Dedup().Fold())
			},
			equals: "g.V('1','2','3').local(out().out().dedup().fold())",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V("3").Out().Path().Count(Scope.Local)
			},
			equals: "g.V('3').out().path().count(local)",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.E().Count()
			},
			equals: "g.E().count()",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V("5").OutE("route").InV().Path().Limit(10)
			},
			equals: "g.V('5').outE('route').inV().path().limit(10)",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V("5").PropertyMap().Select(Column.Keys)
			},
			equals: "g.V('5').propertyMap().select(keys)",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V("5").PropertyMap().Select(Column.Values)
			},
			equals: "g.V('5').propertyMap().select(values)",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V("3").Values("runways").Math("_ + 1")
			},
			equals: "g.V('3').values('runways').math('_ + 1')",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V("3").Emit().Repeat(T__.Out().SimplePath()).Times(3).Limit(5).Path()
			},
			equals: "g.V('3').emit().repeat(out().simplePath()).times(3).limit(5).path()",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V().Match(T__.As("a").Has("code", "LHR").As("b")).Select("b").By("code")
			},
			equals: "g.V().match(as('a').has('code','LHR').as('b')).select('b').by('code')",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V().Has("test-using-keyword-as-property", "repeat")
			},
			equals: "g.V().has('test-using-keyword-as-property','repeat')",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V("1").AddE("test").To(T__.V("4"))
			},
			equals: "g.V('1').addE('test').to(V('4'))",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V().Values("runways").Max()
			},
			equals: "g.V().values('runways').max()",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V().Values("runways").Min()
			},
			equals: "g.V().values('runways').min()",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V().Values("runways").Sum()
			},
			equals: "g.V().values('runways').sum()",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V().Values("runways").Mean()
			},
			equals: "g.V().values('runways').mean()",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.WithSack(0).V("3", "5").Sack(Operator.Sum).By("runways").Sack()
			},
			equals: "g.withSack(0).V('3','5').sack(sum).by('runways').sack()",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.Inject(3, 4, 5)
			},
			equals: "g.inject(3,4,5)",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.Inject([]interface{}{3, 4, 5})
			},
			equals: "g.inject([3,4,5])",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.Inject(3, 4, 5).Count()
			},
			equals: "g.inject(3,4,5).count()",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V().Has("runways", P.Gt(5)).Count()
			},
			equals: "g.V().has('runways',gt(5)).count()",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V().Has("runways", P.Lte(5.3)).Count()
			},
			equals: "g.V().has('runways',lte(5.3)).count()",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V().Has("code", P.Within(123, 124))
			},
			equals: "g.V().has('code',within([123,124]))",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V().Has("code", P.Within(123, "abc"))
			},
			equals: "g.V().has('code',within([123,'abc']))",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V().Has("code", P.Within("abc", 123))
			},
			equals: "g.V().has('code',within(['abc',123]))",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V().Has("code", P.Within("abc", "xyz"))
			},
			equals: "g.V().has('code',within(['abc','xyz']))",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V("1", "2").Has("region", P.Within("US-TX", "US-GA"))
			},
			equals: "g.V('1','2').has('region',within(['US-TX','US-GA']))",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V().And(T__.Has("runways", P.Gt(5)), T__.Has("region", "US-TX"))
			},
			equals: "g.V().and(has('runways',gt(5)),has('region','US-TX'))",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V().Union(T__.Has("runways", P.Gt(5)), T__.Has("region", "US-TX"))
			},
			equals: "g.V().union(has('runways',gt(5)),has('region','US-TX'))",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V("3").Choose(T__.Values("runways").Is(3), T__.Constant("three"), T__.Constant("not three"))
			},
			equals: "g.V('3').choose(values('runways').is(3),constant('three'),constant('not three'))",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V("3").Choose(T__.Values("runways")).Option(1, T__.Constant("three")).Option(2, T__.Constant("not three"))
			},
			equals: "g.V('3').choose(values('runways')).option(1,constant('three')).option(2,constant('not three'))",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V("3").Choose(T__.Values("runways")).Option(1.5, T__.Constant("one and a half")).Option(2, T__.Constant("not three"))
			},
			equals: "g.V('3').choose(values('runways')).option(1.5,constant('one and a half')).option(2,constant('not three'))",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V("3").Repeat(T__.Out().SimplePath()).Until(T__.Loops().Is(1)).Count()
			},
			equals: "g.V('3').repeat(out().simplePath()).until(loops().is(1)).count()",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V().HasLabel("airport").Limit(20).Group().By("region").By("code").Order(Scope.Local).By(Column.Keys)
			},
			equals: "g.V().hasLabel('airport').limit(20).group().by('region').by('code').order(local).by(keys)",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V("1").As("a").V("2").As("a").Select(Pop.All, "a")
			},
			equals: "g.V('1').as('a').V('2').as('a').select(all,'a')",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.AddV("test").Property(Cardinality.Set, "p1", 10)
			},
			equals: "g.addV('test').property(set,'p1',10)",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.AddV("test").Property(Cardinality.List, "p1", 10)
			},
			equals: "g.addV('test').property(list,'p1',10)",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.AddV("test").Property(Cardinality.Single, "p1", 10)
			},
			equals: "g.addV('test').property(single,'p1',10)",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V().Limit(5).Order().By(T__.Label())
			},
			equals: "g.V().limit(5).order().by(label())",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V().Range(1, 5)
			},
			equals: "g.V().range(1,5)",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.AddV("test").Property("p1", 123)
			},
			equals: "g.addV('test').property('p1',123)",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V().Has("date", P.Gt(time.Date(2021, 1, 1, 9, 30, 0, 0, time.UTC)))
			},
			equals: "g.V().has('date',gt(new Date(121,1,1,9,30,0)))",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.AddE("route").From(T__.V("1")).To(T__.V("2"))
			},
			equals: "g.addE('route').from(V('1')).to(V('2'))",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.WithSideEffect("a", []interface{}{1, 2}).V("3").Select("a")
			},
			equals: "g.withSideEffect('a',[1,2]).V('3').select('a')",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.WithSideEffect("a", 1).V("3").Select("a")
			},
			equals: "g.withSideEffect('a',1).V('3').select('a')",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.WithSideEffect("a", "abc").V("3").Select("a")
			},
			equals: "g.withSideEffect('a','abc').V('3').select('a')",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V().Has("airport", "region", "US-NM").Limit(3).Values("elev").Fold().Index()
			},
			equals: "g.V().has('airport','region','US-NM').limit(3).values('elev').fold().index()",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V("3").Repeat(T__.TimeLimit(1000).Out().SimplePath()).Until(T__.Has("code", "AGR")).Path()
			},
			equals: "g.V('3').repeat(timeLimit(1000).out().simplePath()).until(has('code','AGR')).path()",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V().HasLabel("airport").Where(T__.Values("elev").Is(P.Gt(14000)))
			},
			equals: "g.V().hasLabel('airport').where(values('elev').is(gt(14000)))",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V().HasLabel("airport").Where(T__.Out().Count().Is(P.Gt(250))).Values("code")
			},
			equals: "g.V().hasLabel('airport').where(out().count().is(gt(250))).values('code')",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V().HasLabel("airport").Filter(T__.Out().Count().Is(P.Gt(250))).Values("code")
			},
			equals: "g.V().hasLabel('airport').filter(out().count().is(gt(250))).values('code')",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.WithSack(0).
					V("3").
					Repeat(T__.OutE("route").Sack(Operator.Sum).By("dist").InV()).
					Until(T__.Has("code", "AGR").Or().Loops().Is(4)).
					Has("code", "AGR").
					Local(T__.Union(T__.Path().By("code").By("dist"), T__.Sack()).Fold()).
					Limit(10)
			},
			equals: "g.withSack(0).V('3').repeat(outE('route').sack(sum).by('dist').inV()).until(has('code','AGR').or().loops().is(4)).has('code','AGR').local(union(path().by('code').by('dist'),sack()).fold()).limit(10)",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.AddV().As("a").AddV().As("b").AddE("knows").From("a").To("b")
			},
			equals: "g.addV().as('a').addV().as('b').addE('knows').from('a').to('b')",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.AddV("Person").As("a").AddV("Person").As("b").AddE("knows").From("a").To("b")
			},
			equals: "g.addV('Person').as('a').addV('Person').as('b').addE('knows').from('a').to('b')",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V("3").Project("Out", "In").By(T__.Out().Count()).By(T__.In().Count())
			},
			equals: "g.V('3').project('Out','In').by(out().count()).by(in().count())",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V("44").Out().Aggregate("a").Out().Where(P.Within("a")).Path()
			},
			equals: "g.V('44').out().aggregate('a').out().where(within('a')).path()",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V().Has("date", time.Date(2021, 2, 22, 0, 0, 0, 0, time.UTC))
			},
			equals: "g.V().has('date',new Date(121,2,22,0,0,0))",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V().Has("date", P.Within(time.Date(2021, 2, 22, 0, 0, 0, 0, time.UTC), time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)))
			},
			equals: "g.V().has('date',within([new Date(121,2,22,0,0,0),new Date(121,1,1,0,0,0)]))",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V().Has("date", P.Between(time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2021, 2, 22, 0, 0, 0, 0, time.UTC)))
			},
			equals: "g.V().has('date',between(new Date(121,1,1,0,0,0),new Date(121,2,22,0,0,0)))",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V().Has("date", P.Inside(time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2021, 2, 22, 0, 0, 0, 0, time.UTC)))
			},
			equals: "g.V().has('date',inside(new Date(121,1,1,0,0,0),new Date(121,2,22,0,0,0)))",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V().Has("date", P.Gt(time.Date(2021, 1, 1, 9, 30, 0, 0, time.UTC)))
			},
			equals: "g.V().has('date',gt(new Date(121,1,1,9,30,0)))",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V().Has("runways", P.Between(3, 5))
			},
			equals: "g.V().has('runways',between(3,5))",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V().Has("runways", P.Inside(3, 5))
			},
			equals: "g.V().has('runways',inside(3,5))",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V("44").OutE().ElementMap()
			},
			equals: "g.V('44').outE().elementMap()",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V("44").ValueMap().By(T__.Unfold())
			},
			equals: "g.V('44').valueMap().by(unfold())",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V("44").ValueMap().With(WithOptions.Tokens, WithOptions.Labels)
			},
			equals: "g.V('44').valueMap().with(WithOptions.tokens,WithOptions.labels)",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V("44").ValueMap().With(WithOptions.All)
			},
			equals: "g.V('44').valueMap().with(WithOptions.all)",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V("44").ValueMap().With(WithOptions.Indexer)
			},
			equals: "g.V('44').valueMap().with(WithOptions.indexer)",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V("44").ValueMap().With(WithOptions.Tokens)
			},
			equals: "g.V('44').valueMap().with(WithOptions.tokens)",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.WithStrategies(ReadOnlyStrategy()).AddV("test")
			},
			equals: "g.withStrategies(new ReadOnlyStrategy()).addV('test')",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.WithStrategies(SubgraphStrategy(SubgraphStrategyConfig{Vertices: T__.Has("region", "US-TX"), Edges: T__.HasLabel("route")})).V().Count()
			},
			containsRandomClassParams: true,
			equals:                    "g.withStrategies(new SubgraphStrategy(vertices:has('region','US-TX'),edges:hasLabel('route'))).V().count()",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.WithStrategies(SubgraphStrategy(SubgraphStrategyConfig{VertexProperties: T__.HasNot("runways")})).V().Count()
			},
			equals: "g.withStrategies(new SubgraphStrategy(vertexProperties:hasNot('runways'))).V().count()",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.WithStrategies(SubgraphStrategy(SubgraphStrategyConfig{Vertices: T__.Has("region", "US-TX"), VertexProperties: T__.HasNot("runways")})).V().Count()
			},
			containsRandomClassParams: true,
			equals:                    "g.withStrategies(new SubgraphStrategy(vertices:has('region','US-TX'),vertexProperties:hasNot('runways'))).V().count()",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.WithStrategies(ReadOnlyStrategy(), SubgraphStrategy(SubgraphStrategyConfig{Vertices: T__.Has("region", "US-TX"), Edges: T__.HasLabel("route")})).V().Count()
			},
			containsRandomClassParams: true,
			equals:                    "g.withStrategies(new ReadOnlyStrategy(),new SubgraphStrategy(vertices:has('region','US-TX'),edges:hasLabel('route'))).V().count()",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.WithStrategies(ReadOnlyStrategy(), SubgraphStrategy(SubgraphStrategyConfig{Vertices: T__.Has("region", "US-TX")})).V().Count()
			},
			equals: "g.withStrategies(new ReadOnlyStrategy(),new SubgraphStrategy(vertices:has('region','US-TX'))).V().count()",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.WithStrategies(OptionsStrategy(map[string]interface{}{"evaluationTimeout": 500})).V().Count()
			},
			equals: "g.withStrategies(new OptionsStrategy(evaluationTimeout:500)).V().count()",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.WithStrategies(PartitionStrategy(PartitionStrategyConfig{PartitionKey: "partition", WritePartition: "a", ReadPartitions: NewSimpleSet("a")})).AddV("test")
			},
			containsRandomClassParams: true,
			equals:                    "g.withStrategies(new PartitionStrategy(includeMetaProperties:false,partitionKey:'partition',writePartition:'a',readPartitions:['a'])).addV('test')",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.WithStrategies(VertexProgramStrategy(VertexProgramStrategyConfig{})).V().ShortestPath().With("~tinkerpop.shortestPath.target", T__.Has("name", "peter"))
			},
			equals: "g.withStrategies(new VertexProgramStrategy()).V().shortestPath().with('~tinkerpop.shortestPath.target',has('name','peter'))",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V().Has("p1", TextP.StartingWith("foo"))
			},
			equals: "g.V().has('p1',startingWith('foo'))",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V().Has("p1", TextP.EndingWith("foo"))
			},
			equals: "g.V().has('p1',endingWith('foo'))",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V().Has("p1", TextP.Containing("foo"))
			},
			equals: "g.V().has('p1',containing('foo'))",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V().Has("p1", nil)
			},
			equals: "g.V().has('p1',null)",
		},
	}

	var testsToRun []test

	onlyTests := make([]test, 0)
	for _, tt := range tests {
		if tt.only {
			onlyTests = append(onlyTests, tt)
		}
	}

	if len(onlyTests) > 0 {
		testsToRun = onlyTests
	} else {
		testsToRun = tests
	}

	for _, tt := range testsToRun {
		if tt.skip {
			continue
		}

		testName := tt.name
		if testName == "" {
			testName = tt.equals
		}

		t.Run(testName, func(t *testing.T) {
			tr := &translator{
				source: "g",
			}
			g := NewGraphTraversalSource(nil, nil)
			bytecode := tt.assert(g).Bytecode
			got, err := tr.Translate(bytecode)
			if (err != nil) != tt.wantErr {
				t.Errorf("translator.Translate() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.containsRandomClassParams && got != tt.equals {
				t.Errorf("translator.Translate() = %v, equals %v", got, tt.equals)
			}

			if tt.containsRandomClassParams {
				equalsParams := getParams(tt.equals)
				gotParams := getParams(got)

				if len(equalsParams) != len(gotParams) {
					t.Errorf("translator.Translate() = %v, equals %v", got, tt.equals)
				}

				for _, equalsParam := range equalsParams {
					found := false
					for _, gotParam := range gotParams {
						if equalsParam == gotParam {
							found = true
							break
						}
					}
					if !found {
						t.Errorf("translator.Translate() = %v, equals %v", got, tt.equals)
					}
				}
			}
		})
	}
}

func getParams(result string) []string {
	pattern := `(\w+:.*?)`
	re := regexp.MustCompile(pattern)
	matches := re.FindAllStringSubmatch(result, -1)

	params := make([]string, 0)
	for _, match := range matches {
		if len(match) > 0 {
			params = append(params, match[0])
		}
	}

	return params
}
