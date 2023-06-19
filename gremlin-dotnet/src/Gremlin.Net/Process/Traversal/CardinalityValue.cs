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
using System.Threading;

namespace Gremlin.Net.Process.Traversal
{
    /// <summary>
    ///     Holds a property value with the associated <see cref="Traversal.Cardinality" />.
    /// </summary>
    public class CardinalityValue : Bytecode
    {
        /// <summary>
        ///     Gets the <see cref="Traversal.Cardinality" /> for the value.
        /// </summary>
        public Cardinality? Cardinality
        {
            get
            {
                return this.SourceInstructions[0].Arguments[0];
            }
        }

        /// <summary>
        ///     Gets the value.
        /// </summary>
        public object? Value
        {
            get
            {
                return this.SourceInstructions[0].Arguments[1];
            }
        }

        /// <summary>
        ///     Initializes a new instance of the <see cref="CardinalityValue" /> class.
        /// </summary>
        public CardinalityValue(Cardinality card, object val)
        {
            if (card == null)
                throw new ArgumentNullException("Cardinality argument must not be null");
            this.AddSource("CardinalityValueTraversal", card, val);
        }

        /// <summary>
        ///     Creates a new <see cref="CardinalityValue" /> with a particular value and Single <see cref="Cardinality" />.
        /// </summary>
        public static CardinalityValue Single(object value)
        {
            return new CardinalityValue(Cardinality.Single, value);
        }

        /// <summary>
        ///     Creates a new <see cref="CardinalityValue" /> with a particular value and Set <see cref="Cardinality" />.
        /// </summary>
        public static CardinalityValue Set(object value)
        {
            return new CardinalityValue(Cardinality.Set, value);
        }

        /// <summary>
        ///     Creates a new <see cref="CardinalityValue" /> with a particular value and List <see cref="Cardinality" />.
        /// </summary>
        public static CardinalityValue List(object value)
        {
            return new CardinalityValue(Cardinality.List, value);
        }
    }
}