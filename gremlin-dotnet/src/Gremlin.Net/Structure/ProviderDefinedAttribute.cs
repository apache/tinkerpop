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

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Reflection;

namespace Gremlin.Net.Structure
{
    /// <summary>
    /// Marks a class as a provider-defined type target for hydration.
    /// </summary>
    [AttributeUsage(AttributeTargets.Class)]
    public class ProviderDefinedAttribute : Attribute
    {
        /// <summary>
        /// Gets or sets the fully-qualified provider-defined type name.
        /// </summary>
        public string? Name { get; set; }

        /// <summary>
        /// Gets or sets the list of property names to include during dehydration.
        /// If non-null and non-empty, only these properties are serialized.
        /// Cannot be combined with <see cref="ExcludedFields"/>.
        /// </summary>
        public string[]? IncludedFields { get; set; }

        /// <summary>
        /// Gets or sets the list of property names to exclude during dehydration.
        /// If non-null and non-empty, these properties are omitted from serialization.
        /// Cannot be combined with <see cref="IncludedFields"/>.
        /// </summary>
        public string[]? ExcludedFields { get; set; }

        /// <summary>
        /// Static registry of annotated types keyed by PDT name, populated lazily during dehydration.
        /// </summary>
        internal static readonly ConcurrentDictionary<string, Type> RegisteredTypes = new();

        /// <summary>
        /// Hydrates a <see cref="ProviderDefinedType"/> using a registered annotated type.
        /// Returns the original PDT if no annotated type is registered for the name.
        /// </summary>
        internal static object HydrateIfRegistered(ProviderDefinedType pdt)
        {
            if (!RegisteredTypes.TryGetValue(pdt.Name, out var type))
                return pdt;
            var obj = Activator.CreateInstance(type)!;
            foreach (var (key, value) in pdt.Properties)
            {
                var prop = type.GetProperty(key, BindingFlags.Public | BindingFlags.Instance);
                if (prop != null && prop.CanWrite && value != null)
                {
                    prop.SetValue(obj, Convert.ChangeType(value, prop.PropertyType));
                }
            }
            return obj;
        }
    }
}
