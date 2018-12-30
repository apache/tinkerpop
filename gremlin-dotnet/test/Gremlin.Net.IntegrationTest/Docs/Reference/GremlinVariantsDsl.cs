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

using Gremlin.Net.Process.Traversal;
using Gremlin.Net.Structure;

// tag::dsl[]
namespace Dsl 
{
    public static class SocialTraversalExtensions
    {
        public static GraphTraversal<Vertex,Vertex> Knows(this GraphTraversal<Vertex,Vertex> t, string personName) 
        {
            return t.Out("knows").HasLabel("person").Has("name", personName);
        }

        public static GraphTraversal<Vertex, int> YoungestFriendsAge(this GraphTraversal<Vertex,Vertex> t) 
        {
            return t.Out("knows").HasLabel("person").Values<int>("age").Min<int>();
        }

        public static GraphTraversal<Vertex,long> CreatedAtLeast(this GraphTraversal<Vertex,Vertex> t, long number) 
        {
            return t.OutE("created").Count().Is(P.Gte(number));
        }
    }

    public static class __Social 
    {
        public static GraphTraversal<object,Vertex> Knows(string personName)
         {
            return __.Out("knows").HasLabel("person").Has("name", personName);
        }

        public static GraphTraversal<object, int> YoungestFriendsAge() 
        {
            return __.Out("knows").HasLabel("person").Values<int>("age").Min<int>();
        }

        public static GraphTraversal<object,long> CreatedAtLeast(long number) 
        {
            return __.OutE("created").Count().Is(P.Gte(number));
        }
    }

    public static class SocialTraversalSourceExtensions
    {
        public static GraphTraversal<Vertex,Vertex> Persons(this GraphTraversalSource g, params string[] personNames) 
        {
            GraphTraversal<Vertex,Vertex> t = g.V().HasLabel("person");

            if (personNames.Length > 0) 
            {    
                t = t.Has("name", P.Within(personNames));
            }

            return t;
        }
    }
}
// end::dsl[]