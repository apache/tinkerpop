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
using System.Collections.Generic;
using System.Linq;

namespace Gremlin.Net.Structure
{
    /// <summary>
    /// Represents a provider-defined type (PDT) with a name and a set of properties.
    /// </summary>
    public class ProviderDefinedType
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ProviderDefinedType"/> class.
        /// </summary>
        /// <param name="name">The fully-qualified name of the provider-defined type.</param>
        /// <param name="properties">The properties of the provider-defined type.</param>
        public ProviderDefinedType(string name, IReadOnlyDictionary<string, object?> properties)
        {
            Name = name ?? throw new ArgumentNullException(nameof(name));
            if (string.IsNullOrEmpty(name)) throw new ArgumentException("name cannot be empty", nameof(name));
            Properties = properties ?? new Dictionary<string, object?>();
        }

        /// <summary>
        /// Gets the fully-qualified name of this provider-defined type.
        /// </summary>
        public string Name { get; }

        /// <summary>
        /// Gets the properties of this provider-defined type.
        /// </summary>
        public IReadOnlyDictionary<string, object?> Properties { get; }

        /// <inheritdoc />
        public override string ToString() =>
            $"pdt[{Name}]{{{string.Join(", ", Properties.Select(kv => $"{kv.Key}={kv.Value}"))}}}";

        /// <inheritdoc />
        public override bool Equals(object? obj) =>
            obj is ProviderDefinedType other && Name == other.Name &&
            Properties.Count == other.Properties.Count &&
            Properties.All(kv => other.Properties.TryGetValue(kv.Key, out var v) && Equals(kv.Value, v));

        /// <inheritdoc />
        public override int GetHashCode() => HashCode.Combine(Name, Properties.Count);
    }
}
