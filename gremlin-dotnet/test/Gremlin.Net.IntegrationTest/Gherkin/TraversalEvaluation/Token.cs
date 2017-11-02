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
using System.Collections.ObjectModel;
using System.Linq;

namespace Gremlin.Net.IntegrationTest.Gherkin.TraversalEvaluation
{
    internal class Token : IEquatable<Token>
    {
        private static readonly IList<ITokenParameter> EmptyParameters =
            new ReadOnlyCollection<ITokenParameter>(new ITokenParameter[0]);
        
        public bool Equals(Token other)
        {
            if (!string.Equals(Name, other.Name))
            {
                return false;
            }
            return  (Parameters).SequenceEqual(other.Parameters);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != GetType()) return false;
            return Equals((Token) obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return ((Name != null ? Name.GetHashCode() : 0) * 397) ^
                       (Parameters != null ? Parameters.GetHashCode() : 0);
            }
        }

        public string Name { get; }

        /// <summary>
        /// Returns the collection of parameters, can not be null.
        /// </summary>
        public IList<ITokenParameter> Parameters { get; }
            
        public Token(string name, IList<ITokenParameter> parameters = null)
        {
            Name = name;
            Parameters = parameters ?? EmptyParameters;
        }
            
        public Token(string name, ITokenParameter parameter)
        {
            Name = name;
            Parameters = new[] {parameter};
        }

        /// <summary>
        /// Sets the context parameter values by a given name, ie: "v1Id" = 1
        /// </summary>
        public void SetContextParameterValues(IDictionary<string, object> values)
        {
            foreach (var tokenParameter in Parameters)
            {
                tokenParameter.SetContextParameterValues(values);
            }
        }
    }
}