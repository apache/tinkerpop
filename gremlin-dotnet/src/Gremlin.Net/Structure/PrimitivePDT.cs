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

namespace Gremlin.Net.Structure
{
    /// <summary>
    /// Represents a primitive provider-defined type (PDT) with a name and an opaque string value.
    /// </summary>
    public class PrimitivePDT
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="PrimitivePDT"/> class.
        /// </summary>
        /// <param name="name">The fully-qualified name of the provider-defined type.</param>
        /// <param name="value">The opaque string value.</param>
        public PrimitivePDT(string name, string value)
        {
            Name = name ?? throw new ArgumentNullException(nameof(name));
            if (string.IsNullOrEmpty(name)) throw new ArgumentException("name cannot be empty", nameof(name));
            Value = value ?? throw new ArgumentNullException(nameof(value));
        }

        /// <summary>
        /// Gets the fully-qualified name of this primitive provider-defined type.
        /// </summary>
        public string Name { get; }

        /// <summary>
        /// Gets the opaque string value of this primitive provider-defined type.
        /// </summary>
        public string Value { get; }

        /// <inheritdoc />
        public override string ToString() => $"pdt[{Name}]{{{Value}}}";

        /// <inheritdoc />
        public override bool Equals(object? obj) =>
            obj is PrimitivePDT other && Name == other.Name && Value == other.Value;

        /// <inheritdoc />
        public override int GetHashCode() => HashCode.Combine(Name, Value);
    }
}
