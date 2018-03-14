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

namespace Gremlin.Net.Process.Traversal
{
    /// <summary>
    ///     Represents a lambda.
    /// </summary>
    public class Lambda : IFunction, IBiFunction, IPredicate, IUnaryOperator, IBinaryOperator, IComparator, IConsumer,
        ISupplier
    {
        private const int DefaultArgument = -1;

        private Lambda(string expression, string language)
        {
            LambdaExpression = expression;
            Language = language;
        }

        /// <summary>
        ///     Gets the lambda expression.
        /// </summary>
        public string LambdaExpression { get; }

        /// <summary>
        ///     Gets the language of this lambda.
        /// </summary>
        public string Language { get; }

        internal object Arguments => DefaultArgument;

        /// <summary>
        ///     Creates a new Groovy <see cref="Lambda"/>.
        /// </summary>
        /// <param name="expression">The lambda expression.</param>
        /// <returns>The created <see cref="Lambda"/>.</returns>
        public static Lambda Groovy(string expression)
        {
            return new Lambda(expression, "gremlin-groovy");
        }

        /// <summary>
        ///     Creates a new Python <see cref="Lambda"/>.
        /// </summary>
        /// <param name="expression">The lambda expression.</param>
        /// <returns>The created <see cref="Lambda"/>.</returns>
        public static Lambda Python(string expression)
        {
            return new Lambda(expression, "gremlin-python");
        }
    }
}