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

namespace Gremlin.Net.Process.Traversal
{
    /// <summary>
    ///     Holds a property value with the associated <see cref="Traversal.Cardinality" />.
    /// </summary>
    public class CardinalityValue
    {
        /// <summary>
        ///     Gets the <see cref="Traversal.Cardinality" /> for the value.
        /// </summary>
        public Cardinality? Cardinality { get; }

        /// <summary>
        ///     Gets the value.
        /// </summary>
        public object? Value { get; }

        /// <summary>
        ///     Initializes a new instance of the <see cref="CardinalityValue" /> class.
        /// </summary>
        /// <param name="cardinality">The cardinality for the value.</param>
        /// <param name="value">The property value.</param>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="cardinality" /> is null.</exception>
        public CardinalityValue(Cardinality cardinality, object? value)
        {
            if (cardinality == null)
                throw new ArgumentNullException(nameof(cardinality), "Cardinality argument must not be null");
            Cardinality = cardinality;
            Value = value;
        }

        /// <summary>
        ///     Creates a new <see cref="CardinalityValue" /> with a particular value and Single <see cref="Traversal.Cardinality" />.
        /// </summary>
        public static CardinalityValue Single(object? value)
        {
            return new CardinalityValue(Traversal.Cardinality.Single, value);
        }

        /// <summary>
        ///     Creates a new <see cref="CardinalityValue" /> with a particular value and Set <see cref="Traversal.Cardinality" />.
        /// </summary>
        public static CardinalityValue Set(object? value)
        {
            return new CardinalityValue(Traversal.Cardinality.Set, value);
        }

        /// <summary>
        ///     Creates a new <see cref="CardinalityValue" /> with a particular value and List <see cref="Traversal.Cardinality" />.
        /// </summary>
        public static CardinalityValue List(object? value)
        {
            return new CardinalityValue(Traversal.Cardinality.List, value);
        }
    }
}
