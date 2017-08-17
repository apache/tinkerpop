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
using System.Threading;

namespace Gremlin.Net.Process.Traversal
{
    /// <summary>
    ///     Bindings are used to associate a variable with a value.
    /// </summary>
    public class Bindings
    {
        /// <summary>
        ///     Gets an instance of the <see cref="Bindings" /> class.
        /// </summary>
        public static Bindings Instance { get; } = new Bindings();

        private static readonly ThreadLocal<Dictionary<object, string>> BoundVariableByValue =
            new ThreadLocal<Dictionary<object, string>>();

        /// <summary>
        ///     Binds the variable to the specified value.
        /// </summary>
        /// <param name="variable">The variable to bind.</param>
        /// <param name="value">The value to which the variable should be bound.</param>
        /// <returns>The bound value.</returns>
        public TV Of<TV>(string variable, TV value)
        {
            var dict = BoundVariableByValue.Value;
            if (dict == null)
            {
                dict = new Dictionary<object, string>();
                BoundVariableByValue.Value = dict;
            }
            dict[value] = variable;
            return value;
        }

        internal static string GetBoundVariable<TV>(TV value)
        {
            var dict = BoundVariableByValue.Value;
            if (dict == null)
                return null;
            return !dict.ContainsKey(value) ? null : dict[value];
        }

        internal static void Clear()
        {
            BoundVariableByValue.Value?.Clear();
        }
    }
}