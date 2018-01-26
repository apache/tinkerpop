/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.tinkerpop.gremlin.sparql;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.jena.graph.Triple;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.SortCondition;
import org.apache.jena.query.Syntax;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpVisitorBase;
import org.apache.jena.sparql.algebra.OpWalker;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpFilter;
import org.apache.jena.sparql.algebra.op.OpUnion;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.core.VarExprList;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.expr.ExprAggregator;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;

/**
 * The engine that transpiles SPARQL to Gremlin traversals thus enabling SPARQL to be executed on any TinkerPop-enabled
 * graph system.
 */
public class SparqlToGremlinTranspiler {

	private GraphTraversal<Vertex, ?> traversal;

    private List<Traversal> traversalList = new ArrayList<>();

    private String sortingVariable = "";

	private SparqlToGremlinTranspiler(final GraphTraversal<Vertex, ?> traversal) {
		this.traversal = traversal;
	}

	private SparqlToGremlinTranspiler(final GraphTraversalSource g) {
		this(g.V());
	}

    /**
     * Converts SPARQL to a Gremlin traversal.
     *
     * @param graph the {@link Graph} instance to execute the traversal from
     * @param sparqlQuery the query to transpile to Gremlin
     */
    public static GraphTraversal<Vertex, ?> transpile(final Graph graph, final String sparqlQuery) {
        return transpile(graph.traversal(),	sparqlQuery);
    }

    /**
     * Converts SPARQL to a Gremlin traversal.
     *
     * @param g the {@link GraphTraversalSource} instance to execute the traversal from
     * @param sparqlQuery the query to transpile to Gremlin
     */
    public static GraphTraversal<Vertex, ?> transpile(final GraphTraversalSource g, final String sparqlQuery) {
        return transpile(g, QueryFactory.create(Prefixes.prepend(sparqlQuery), Syntax.syntaxSPARQL));
    }

	private GraphTraversal<Vertex, ?> transpile(final Query query) {
		final Op op = Algebra.compile(query);
		OpWalker.walk(op, new GremlinOpVisitor());

		int traversalIndex = 0;
		final int numberOfTraversal = traversalList.size();
        final Traversal arrayOfAllTraversals[] = new Traversal[numberOfTraversal];

		if (query.hasOrderBy() && !query.hasGroupBy()) {
            final List<SortCondition> sortingConditions = query.getOrderBy();

			for (SortCondition sortCondition : sortingConditions) {
                final Expr expr = sortCondition.getExpression();
				sortingVariable = expr.getVarName();
			}
		}

		for (Traversal tempTrav : traversalList) {
			arrayOfAllTraversals[traversalIndex++] = tempTrav;
		}

		Order orderDirection = Order.incr;
		if (query.hasOrderBy()) {
            int directionOfSort = 0;
            final List<SortCondition> sortingConditions = query.getOrderBy();

			for (SortCondition sortCondition : sortingConditions) {
                final Expr expr = sortCondition.getExpression();
				directionOfSort = sortCondition.getDirection();
				sortingVariable = expr.getVarName();
			}

			if (directionOfSort == -1) orderDirection = Order.decr;
		}

		if (traversalList.size() > 0)
			traversal = traversal.match(arrayOfAllTraversals);

		final List<String> vars = query.getResultVars();
		if (!query.isQueryResultStar() && !query.hasGroupBy()) {
            switch (vars.size()) {
                case 0:
                    throw new IllegalStateException();
                case 1:
                    if (query.isDistinct())
                        traversal = traversal.dedup(vars.get(0));

                    if (query.hasOrderBy())
                        traversal = traversal.order().by(sortingVariable, orderDirection);
                    else
                        traversal = traversal.select(vars.get(0));

                    break;
                case 2:
                    if (query.isDistinct())
                        traversal = traversal.dedup(vars.get(0), vars.get(1));

                    if (query.hasOrderBy())
                        traversal = traversal.order().by(__.select(vars.get(0)), orderDirection).by(__.select(vars.get(1)));
                    else
                        traversal = traversal.select(vars.get(0), vars.get(1));

                    break;
                default:
                    final String[] all = new String[vars.size()];
                    vars.toArray(all);
                    if (query.isDistinct())
                        traversal = traversal.dedup(all);

                    final String[] others = Arrays.copyOfRange(all, 2, vars.size());
                    if (query.hasOrderBy())
                        traversal = traversal.order().by(__.select(vars.get(0)), orderDirection).by(__.select(vars.get(1)));
                    else
                        traversal = traversal.select(vars.get(0), vars.get(1), others);

                    break;
            }
		}

		if (query.hasGroupBy()) {
			final VarExprList lstExpr = query.getGroupBy();
			String grpVar = "";
			for (Var expr : lstExpr.getVars()) {
				grpVar = expr.getName();
			}

			if (!grpVar.isEmpty())
				traversal = traversal.select(grpVar);
			if (query.hasAggregators()) {
                final List<ExprAggregator> exprAgg = query.getAggregators();
				for (ExprAggregator expr : exprAgg) {
					if (expr.getAggregator().getName().contains("COUNT")) {
						if (!query.toString().contains("GROUP")) {
							if (expr.getAggregator().toString().contains("DISTINCT"))
								traversal = traversal.dedup(expr.getAggregator().getExprList().get(0).toString().substring(1));
							else
								traversal = traversal.select(expr.getAggregator().getExprList().get(0).toString().substring(1));

							traversal = traversal.count();
						} else {
                            traversal = traversal.groupCount();
                        }
					}

					if (expr.getAggregator().getName().contains("MAX")) {
						traversal = traversal.max();
					}
				}
			} else {
                traversal = traversal.group();
            }
		}

		if (query.hasOrderBy() && query.hasGroupBy())
			traversal = traversal.order().by(sortingVariable, orderDirection);

		if (query.hasLimit()) {
			long limit = query.getLimit(), offset = 0;

			if (query.hasOffset())
				offset = query.getOffset();

			if (query.hasGroupBy() && query.hasOrderBy())
				traversal = traversal.range(Scope.local, offset, offset + limit);
			else
				traversal = traversal.range(offset, offset + limit);
		}

		return traversal;
	}

    private static GraphTraversal<Vertex, ?> transpile(final GraphTraversalSource g, final Query query) {
        return new SparqlToGremlinTranspiler(g).transpile(query);
    }

    /**
     * An {@code OpVisitor} implementation that reads SPARQL algebra operations into Gremlin traversals.
     */
    private class GremlinOpVisitor extends OpVisitorBase {

        /**
         * Visiting triple patterns in SPARQL algebra.
         */
        @Override
        public void visit(final OpBGP opBGP) {
            final List<Triple> triples = opBGP.getPattern().getList();
            final Traversal[] matchTraversals = new Traversal[triples.size()];
            int i = 0;
            for (final Triple triple : triples) {

                matchTraversals[i++] = TraversalBuilder.transform(triple);
                traversalList.add(matchTraversals[i - 1]);
            }
        }

        /**
         * Visiting filters in SPARQL algebra.
         */
        @Override
        public void visit(final OpFilter opFilter) {
            Traversal traversal;
            for (Expr expr : opFilter.getExprs().getList()) {
                if (expr != null) {
                    traversal = __.where(WhereTraversalBuilder.transform(expr));
                    traversalList.add(traversal);
                }
            }
        }

        /**
         * Visiting unions in SPARQL algebra.
         */
        @Override
        public void visit(final OpUnion opUnion) {
            final Traversal unionTemp[] = new Traversal[2];
            final Traversal unionTemp1[] = new Traversal[traversalList.size() / 2];
            final Traversal unionTemp2[] = new Traversal[traversalList.size() / 2];

            int count = 0;

            for (int i = 0; i < traversalList.size(); i++) {
                if (i < traversalList.size() / 2)
                    unionTemp1[i] = traversalList.get(i);
                else
                    unionTemp2[count++] = traversalList.get(i);
            }

            unionTemp[1] = __.match(unionTemp2);
            unionTemp[0] = __.match(unionTemp1);

            traversalList.clear();
            traversal = (GraphTraversal<Vertex, ?>) traversal.union(unionTemp);
        }

    }
}
