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

namespace Gremlin.Net.Structure
{
    /// <summary>
    /// Adapter for hydrating a <see cref="PrimitiveProviderDefinedType"/> into a strongly-typed object.
    /// </summary>
    /// <typeparam name="T">The target type to hydrate into.</typeparam>
    public interface IPrimitivePdtAdapter<T>
    {
        /// <summary>
        /// Gets the fully-qualified type name this adapter handles.
        /// </summary>
        string TypeName { get; }

        /// <summary>
        /// Creates a typed instance from the opaque string value.
        /// </summary>
        T FromString(string value);

        /// <summary>
        /// Converts a typed instance to its opaque string representation.
        /// </summary>
        string ToString(T obj);
    }
}
