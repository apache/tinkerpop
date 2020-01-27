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

import java.util.List;

import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.expr.E_Equals;
import org.apache.jena.sparql.expr.E_Exists;
import org.apache.jena.sparql.expr.E_GreaterThan;
import org.apache.jena.sparql.expr.E_GreaterThanOrEqual;
import org.apache.jena.sparql.expr.E_LessThan;
import org.apache.jena.sparql.expr.E_LessThanOrEqual;
import org.apache.jena.sparql.expr.E_LogicalAnd;
import org.apache.jena.sparql.expr.E_LogicalOr;
import org.apache.jena.sparql.expr.E_NotEquals;
import org.apache.jena.sparql.expr.E_NotExists;
import org.apache.jena.sparql.expr.E_StrLength;
import org.apache.jena.sparql.expr.Expr;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;

/**
 * Converts SPARQL "where" expressions to Gremlin predicates.
 */
class WhereTraversalBuilder {

    /**
     * Converts a general {@code Expr} to an anonymous {@link GraphTraversal}.
     */
    public static GraphTraversal<?, ?> transform(final Expr expression, List<Triple> triples) {
        if (expression instanceof E_Equals) return transform((E_Equals) expression, triples);
        if (expression instanceof E_NotEquals) return transform((E_NotEquals) expression, triples);
        if (expression instanceof E_LessThan) return transform((E_LessThan) expression, triples);
        if (expression instanceof E_LessThanOrEqual) return transform((E_LessThanOrEqual) expression, triples);
        if (expression instanceof E_GreaterThan) return transform((E_GreaterThan) expression, triples);
        if (expression instanceof E_GreaterThanOrEqual) return transform((E_GreaterThanOrEqual) expression, triples);
        if (expression instanceof E_LogicalAnd) return transform((E_LogicalAnd) expression, triples);
        if (expression instanceof E_LogicalOr) return transform((E_LogicalOr) expression, triples);
        if (expression instanceof E_Exists) return transform((E_Exists) expression, triples);
        if (expression instanceof E_NotExists) return transform((E_NotExists) expression, triples);
        throw new IllegalStateException(String.format("Unhandled expression: %s", expression));
    }

   public static GraphTraversal<?, ?> transform(final E_Equals e, List<Triple> triples) {
        GraphTraversal traversal = null;
         for(final Triple triple : triples){

            String subject = "";
            if( triple.getSubject().isVariable()) {        
	            subject = triple.getSubject().getName().toString();
        	}
        	else {
        		subject = triple.getSubject().getLiteralValue().toString();
        	}

            String object = "";
            if( triple.getObject().isVariable()) {        
	            object = triple.getObject().getName().toString();
        	}
        	else {
        		object = triple.getObject().getLiteralValue().toString();
        	}

            String uri = Prefixes.getURIValue(triple.getPredicate().getURI());
            String arg1 = e.getArg1().getExprVar().getVarName();
            
            final Object value =  e.getArg2().getConstant().getNode().getLiteralValue();

            if (object.equals(arg1)){
               return __.as(subject).has(uri, P.eq(value));
            }
        }
        return traversal;
    }
 public static GraphTraversal<?, ?> transform( final E_NotEquals e, List<Triple> triples) {
        GraphTraversal traversal = null;
         for(final Triple triple : triples){

            String subject = "";
            if( triple.getSubject().isVariable()) {        
	            subject = triple.getSubject().getName().toString();
        	}
        	else {
        		subject = triple.getSubject().getLiteralValue().toString();
        	}

            String object = "";
            if( triple.getObject().isVariable()) {        
	            object = triple.getObject().getName().toString();
        	}
        	else {
        		object = triple.getObject().getLiteralValue().toString();
        	}

            String uri = Prefixes.getURIValue(triple.getPredicate().getURI());
            String arg1 = e.getArg1().getExprVar().getVarName();
            
            final Object value =  e.getArg2().getConstant().getNode().getLiteralValue();

            if (object.equals(arg1)){
                return __.as(subject).has(uri, P.neq(value));  
            }
        }
        return traversal;
    }

    public static GraphTraversal<?, ?> transform( final E_LessThan e, List<Triple> triples) {
 GraphTraversal traversal = null;
         for(final Triple triple : triples){

            String subject = "";
            if( triple.getSubject().isVariable()) {        
	            subject = triple.getSubject().getName().toString();
        	}
        	else {
        		subject = triple.getSubject().getLiteralValue().toString();
        	}

            String object = "";
            if( triple.getObject().isVariable()) {        
	            object = triple.getObject().getName().toString();
        	}
        	else {
        		object = triple.getObject().getLiteralValue().toString();
        	}

            String uri = Prefixes.getURIValue(triple.getPredicate().getURI());
            String arg1 = e.getArg1().getExprVar().getVarName();
            
            final Object value =  e.getArg2().getConstant().getNode().getLiteralValue();

            if (object.equals(arg1)){
               return __.as(subject).has(uri, P.lt(value));
            }
        }
        return traversal;
    }

    public static GraphTraversal<?, ?> transform(final E_LessThanOrEqual e, List<Triple> triples) {
GraphTraversal traversal = null;
         for(final Triple triple : triples){

            String subject = "";
            if( triple.getSubject().isVariable()) {        
	            subject = triple.getSubject().getName().toString();
        	}
        	else {
        		subject = triple.getSubject().getLiteralValue().toString();
        	}

            String object = "";
            if( triple.getObject().isVariable()) {        
	            object = triple.getObject().getName().toString();
        	}
        	else {
        		object = triple.getObject().getLiteralValue().toString();
        	}

            String uri = Prefixes.getURIValue(triple.getPredicate().getURI());
            String arg1 = e.getArg1().getExprVar().getVarName();
            
            final Object value =  e.getArg2().getConstant().getNode().getLiteralValue();

            if (object.equals(arg1)){
                return __.as(subject).has(uri, P.lte(value));
            }
        }
        return traversal;
    }

    public static GraphTraversal<?, ?> transform(final E_GreaterThan e, List<Triple> triples) {
        GraphTraversal traversal = null;
         for(final Triple triple : triples){

            String subject = "";
            if( triple.getSubject().isVariable()) {        
	            subject = triple.getSubject().getName().toString();
        	}
        	else {
        		subject = triple.getSubject().getLiteralValue().toString();
        	}

            String object = "";
            if( triple.getObject().isVariable()) {        
	            object = triple.getObject().getName().toString();
        	}
        	else {
        		object = triple.getObject().getLiteralValue().toString();
        	}

            String uri = Prefixes.getURIValue(triple.getPredicate().getURI());
            String arg1 = e.getArg1().getExprVar().getVarName();
            
            final Object value =  e.getArg2().getConstant().getNode().getLiteralValue();

            if (object.equals(arg1)){
                return __.as(subject).has(uri, P.gt(value));
            }
        }
        return traversal;
    }

    public static GraphTraversal<?, ?> transform(final E_GreaterThanOrEqual e, List<Triple> triples) {
         GraphTraversal traversal = null;
         for(final Triple triple : triples){

            String subject = "";
            if( triple.getSubject().isVariable()) {        
	            subject = triple.getSubject().getName().toString();
        	}
        	else {
        		subject = triple.getSubject().getLiteralValue().toString();
        	}

            String object = "";
            if( triple.getObject().isVariable()) {        
	            object = triple.getObject().getName().toString();
        	}
        	else {
        		object = triple.getObject().getLiteralValue().toString();
        	}

            String uri = Prefixes.getURIValue(triple.getPredicate().getURI());
            String arg1 = e.getArg1().getExprVar().getVarName();
            
            final Object value =  e.getArg2().getConstant().getNode().getLiteralValue();

            if (object.equals(arg1)){
                return __.as(subject).has(uri, P.gte(value));
            }
        }
        return traversal;
    }



  public static int getStrLength(final Triple triple, final E_StrLength expression){
    	
    	return expression.getArg().toString().length();
    	
    }
    public static GraphTraversal<?, ?> transform(final E_LogicalAnd expression, List<Triple> triples) {
        
        return __.and(
                transform(expression.getArg1(),triples),
                transform(expression.getArg2(),triples));
    }

    public static GraphTraversal<?, ?> transform(final E_LogicalOr expression, List<Triple> triples) {
        return __.or(
                transform(expression.getArg1(),triples),
                transform(expression.getArg2(),triples));
    }

    public static GraphTraversal<?, ?> transform(final E_Exists expression, List<Triple> triples) {
        final OpBGP opBGP = (OpBGP) expression.getGraphPattern();
        final List<Triple> this_triples = opBGP.getPattern().getList();
        if ( this_triples.size() != 1) throw new IllegalStateException("Unhandled EXISTS pattern");
        final GraphTraversal<?, ?> traversal = TraversalBuilder.transform( this_triples.get(0));
        final Step endStep = traversal.asAdmin().getEndStep();
        final String label = (String) endStep.getLabels().iterator().next();
        endStep.removeLabel(label);
        return traversal;
    }
    

    public static GraphTraversal<?, ?> transform(final E_NotExists expression, List<Triple> triples) {
        final OpBGP opBGP = (OpBGP) expression.getGraphPattern();
        final List<Triple>  this_triples = opBGP.getPattern().getList();
        if ( this_triples.size() != 1) throw new IllegalStateException("Unhandled NOT EXISTS pattern");
        final GraphTraversal<?, ?> traversal = TraversalBuilder.transform( this_triples.get(0));
        final Step endStep = traversal.asAdmin().getEndStep();
        final String label = (String) endStep.getLabels().iterator().next();
        endStep.removeLabel(label);
        return __.not(traversal);
    }
}
