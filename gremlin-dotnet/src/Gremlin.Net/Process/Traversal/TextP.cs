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

using System.Collections.Generic;
using System.Linq;

namespace Gremlin.Net.Process.Traversal
{
#pragma warning disable 1591

    /// <summary>
    ///     A <see cref="TextP" /> is a predicate of the form Func&lt;string, bool&gt;.
    ///     That is, given some string, return true or false.
    /// </summary>
    public class TextP : P
    {
        /// <summary>
        ///     Initializes a new instance of the <see cref="TextP" /> class.
        /// </summary>
        /// <param name="operatorName">The name of the predicate.</param>
        /// <param name="value">The value of the predicate.</param>
        /// <param name="other">An optional other predicate that is used as an argument for this predicate.</param>
        public TextP(string operatorName, string value, P other = null) : base(operatorName, value, other)
        {
        }


        public static TextP Containing(string value)
        {
            return new TextP("containing", value);
        }

        public static TextP EndingWith(string value)
        {
            return new TextP("endingWith", value);
        }

        public static TextP NotContaining(string value)
        {
            return new TextP("notContaining", value);
        }

        public static TextP NotEndingWith(string value)
        {
            return new TextP("notEndingWith", value);
        }

        public static TextP NotStartingWith(string value)
        {
            return new TextP("notStartingWith", value);
        }

        public static TextP StartingWith(string value)
        {
            return new TextP("startingWith", value);
        }


        private static T[] ToGenericArray<T>(ICollection<T> collection)
        {
            return collection?.ToArray() ?? new T[0];
        }

        /// <inheritdoc />
        public override string ToString()
        {
            return Other == null ? $"{OperatorName}({Value})" : $"{OperatorName}({Value},{Other})";
        }
    }

#pragma warning restore 1591
}
