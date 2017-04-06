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

namespace Gremlin.Net.Structure.IO.GraphSON
{
    /// <summary>
    ///     Provides helper methods for GraphSON serialization.
    /// </summary>
    public static class GraphSONUtil
    {
        /// <summary>
        ///     Transforms a value intos its GraphSON representation including type information.
        /// </summary>
        /// <param name="typename">The name of the type.</param>
        /// <param name="value">The value to transform.</param>
        /// <param name="prefix">A namespace prefix for the typename.</param>
        /// <returns>The GraphSON representation including type information.</returns>
        public static Dictionary<string, dynamic> ToTypedValue(string typename, dynamic value, string prefix = "g")
        {
            var typedValue = new Dictionary<string, dynamic>
            {
                {GraphSONTokens.TypeKey, FormatTypeName(prefix, typename)}
            };
            if (value != null)
                typedValue[GraphSONTokens.ValueKey] = value;
            return typedValue;
        }

        /// <summary>
        ///     Formats a type name with its prefix to a GraphSON TypeID.
        /// </summary>
        /// <param name="namespacePrefix">The namespace prefix (default is "g").</param>
        /// <param name="typeName">The name of the type.</param>
        /// <returns>The formatted TypeID.</returns>
        public static string FormatTypeName(string namespacePrefix, string typeName)
        {
            return $"{namespacePrefix}:{typeName}";
        }
    }
}