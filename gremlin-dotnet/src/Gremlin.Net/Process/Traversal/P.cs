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

// THIS IS A GENERATED FILE - DO NOT MODIFY THIS FILE DIRECTLY - see pom.xml
namespace Gremlin.Net.Process.Traversal
{
    /// <summary>
    ///     A <see cref="P" /> is a predicate of the form Func&lt;object, bool&gt;.
    ///     That is, given some object, return true or false.
    /// </summary>
    public class P
    {

        public static TraversalPredicate Between(params object[] args)
        {
            var value = args.Length == 1 ? args[0] : args;
            return new TraversalPredicate("between", value);
        }

        public static TraversalPredicate Eq(params object[] args)
        {
            var value = args.Length == 1 ? args[0] : args;
            return new TraversalPredicate("eq", value);
        }

        public static TraversalPredicate Gt(params object[] args)
        {
            var value = args.Length == 1 ? args[0] : args;
            return new TraversalPredicate("gt", value);
        }

        public static TraversalPredicate Gte(params object[] args)
        {
            var value = args.Length == 1 ? args[0] : args;
            return new TraversalPredicate("gte", value);
        }

        public static TraversalPredicate Inside(params object[] args)
        {
            var value = args.Length == 1 ? args[0] : args;
            return new TraversalPredicate("inside", value);
        }

        public static TraversalPredicate Lt(params object[] args)
        {
            var value = args.Length == 1 ? args[0] : args;
            return new TraversalPredicate("lt", value);
        }

        public static TraversalPredicate Lte(params object[] args)
        {
            var value = args.Length == 1 ? args[0] : args;
            return new TraversalPredicate("lte", value);
        }

        public static TraversalPredicate Neq(params object[] args)
        {
            var value = args.Length == 1 ? args[0] : args;
            return new TraversalPredicate("neq", value);
        }

        public static TraversalPredicate Not(params object[] args)
        {
            var value = args.Length == 1 ? args[0] : args;
            return new TraversalPredicate("not", value);
        }

        public static TraversalPredicate Outside(params object[] args)
        {
            var value = args.Length == 1 ? args[0] : args;
            return new TraversalPredicate("outside", value);
        }

        public static TraversalPredicate Test(params object[] args)
        {
            var value = args.Length == 1 ? args[0] : args;
            return new TraversalPredicate("test", value);
        }

        public static TraversalPredicate Within(params object[] args)
        {
            var value = args.Length == 1 ? args[0] : args;
            return new TraversalPredicate("within", value);
        }

        public static TraversalPredicate Without(params object[] args)
        {
            var value = args.Length == 1 ? args[0] : args;
            return new TraversalPredicate("without", value);
        }

    }
}