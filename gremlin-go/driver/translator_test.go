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

import "testing"

func Test_translator_Translate(t *testing.T) {
	type test struct {
		name    string
		assert  func(g *GraphTraversalSource) *GraphTraversal
		equals  string
		only    bool
		skip    bool
		wantErr bool
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
			equals: "g.V('1').addE('test').to(__.V('4'))",
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
			equals: "g.withSack(0).V('3','5').sack(Operator.sum).by('runways').sack()",
		},
		{
			assert: func(g *GraphTraversalSource) *GraphTraversal {
				return g.V("3").Values("runways").Store("x").V("4").Values("runways").Store("x").By(T__.Constant(1)).V("6").Store("x").By(T__.Constant(1)).Select("x").Unfold().Sum()
			},
			equals: "g.V('3').values('runways').store('x').V('4').values('runways').store('x').by(__.constant(1)).V('6').store('x').by(__.constant(1)).select('x').unfold().sum()",
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
			if got != tt.equals {
				t.Errorf("translator.Translate() = %v, equals %v", got, tt.equals)
			}
		})
	}
}
