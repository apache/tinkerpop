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

using Gremlin.CSharp.Process;
using Xunit;

namespace Gremlin.CSharp.UnitTest
{
    public class PredicateTests
    {
        [Fact]
        public void ShouldKeepOrderForNestedPredicate()
        {
            Assert.Equal("and(eq(a),lt(b))", P.Eq("a").And(P.Lt("b")).ToString());
        }

        [Fact]
        public void ShouldKeepOrderForDoubleNestedPredicate()
        {
            Assert.Equal("and(or(lt(b),gt(c)),neq(d))", P.Lt("b").Or(P.Gt("c")).And(P.Neq("d")).ToString());
        }

        [Fact]
        public void ShouldKeepOrderForTripleNestedPredicate()
        {
            Assert.Equal("and(or(lt(b),gt(c)),or(neq(d),gte(e)))",
                P.Lt("b").Or(P.Gt("c")).And(P.Neq("d").Or(P.Gte("e"))).ToString());
        }
    }
}