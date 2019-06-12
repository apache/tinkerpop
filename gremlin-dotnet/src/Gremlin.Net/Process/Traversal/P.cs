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
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace Gremlin.Net.Process.Traversal
{
#pragma warning disable 1591

    /// <summary>
    ///     A <see cref="P" /> is a predicate of the form Func&lt;object, bool&gt;.
    ///     That is, given some object, return true or false.
    /// </summary>
    public class P : IPredicate
    {
        /// <summary>
        ///     Initializes a new instance of the <see cref="P" /> class.
        /// </summary>
        /// <param name="operatorName">The name of the predicate.</param>
        /// <param name="value">The value of the predicate.</param>
        /// <param name="other">An optional other predicate that is used as an argument for this predicate.</param>
        public P(string operatorName, dynamic value, P other = null)
        {
            OperatorName = operatorName;
            Value = value;
            Other = other;
        }

        /// <summary>
        ///     Gets the name of the predicate.
        /// </summary>
        public string OperatorName { get; }

        /// <summary>
        ///     Gets the value of the predicate.
        /// </summary>
        public dynamic Value { get; }

        /// <summary>
        ///     Gets an optional other predicate that is used as an argument for this predicate.
        /// </summary>
        public P Other { get; }

        /// <summary>
        ///     Returns a composed predicate that represents a logical AND of this predicate and another.
        /// </summary>
        /// <param name="otherPredicate">A predicate that will be logically-ANDed with this predicate.</param>
        /// <returns>The composed predicate.</returns>
        public P And(P otherPredicate)
        {
            return new P("and", this, otherPredicate);
        }

        /// <summary>
        ///     Returns a composed predicate that represents a logical OR of this predicate and another.
        /// </summary>
        /// <param name="otherPredicate">A predicate that will be logically-ORed with this predicate.</param>
        /// <returns>The composed predicate.</returns>
        public P Or(P otherPredicate)
        {
            return new P("or", this, otherPredicate);
        }

        public static P Between(params object[] args)
        {
            var value = args.Length == 1 ? args[0] : args;
            return new P("between", value);
        }

        public static P Eq(params object[] args)
        {
            var value = args.Length == 1 ? args[0] : args;
            return new P("eq", value);
        }

        public static P Gt(params object[] args)
        {
            var value = args.Length == 1 ? args[0] : args;
            return new P("gt", value);
        }

        public static P Gte(params object[] args)
        {
            var value = args.Length == 1 ? args[0] : args;
            return new P("gte", value);
        }

        public static P Inside(params object[] args)
        {
            var value = args.Length == 1 ? args[0] : args;
            return new P("inside", value);
        }

        public static P Lt(params object[] args)
        {
            var value = args.Length == 1 ? args[0] : args;
            return new P("lt", value);
        }

        public static P Lte(params object[] args)
        {
            var value = args.Length == 1 ? args[0] : args;
            return new P("lte", value);
        }

        public static P Neq(params object[] args)
        {
            var value = args.Length == 1 ? args[0] : args;
            return new P("neq", value);
        }

        public static P Not(params object[] args)
        {
            var value = args.Length == 1 ? args[0] : args;
            return new P("not", value);
        }

        public static P Outside(params object[] args)
        {
            var value = args.Length == 1 ? args[0] : args;
            return new P("outside", value);
        }

        public static P Test(params object[] args)
        {
            var value = args.Length == 1 ? args[0] : args;
            return new P("test", value);
        }

        public static P Within(params object[] args)
        {
            var x = args.Length == 1 && args[0] is ICollection collection ? collection : args;
            return new P("within", ToGenericList(x));
        }

        public static P Without(params object[] args)
        {
            var x = args.Length == 1 && args[0] is ICollection collection ? collection : args;
            return new P("without", ToGenericList(x));
        }


        private static List<object> ToGenericList(IEnumerable collection)
        {
            return collection?.Cast<object>().ToList() ?? Enumerable.Empty<object>().ToList();
        }

        /// <inheritdoc />
        public override string ToString()
        {
            return Other == null ? $"{OperatorName}({Value})" : $"{OperatorName}({Value},{Other})";
        }
    }

#pragma warning restore 1591
}