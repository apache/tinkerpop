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

namespace Gremlin.Net.IntegrationTest.Gherkin.TraversalEvaluation
{
    /// <summary>
    /// Represents a parameter for a traversal predicate (ie: P.gt())
    /// </summary>
    internal class TraversalPredicateParameter : ITokenParameter, IEquatable<TraversalPredicateParameter>
    {
        public bool Equals(TraversalPredicateParameter other)
        {
            return Parts.SequenceEqual(other.Parts);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != GetType()) return false;
            return Equals((TraversalPredicateParameter) obj);
        }

        public override int GetHashCode()
        {
            return Parts != null ? Parts.GetHashCode() : 0;
        }

        public IList<Token> Parts { get; }
        
        public TraversalPredicateParameter(IList<Token> parts)
        {
            Parts = parts;
        }
    }
}