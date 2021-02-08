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
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;
using Xunit;
using Gherkin;
using Gherkin.Ast;
using Gremlin.Net.IntegrationTest.Gherkin.Attributes;
using Xunit.Abstractions;

namespace Gremlin.Net.IntegrationTest.Gherkin
{
    public class GherkinTestRunner
    {
        private static readonly IDictionary<string, IgnoreReason> IgnoredScenarios =
            new Dictionary<string, IgnoreReason>
            {
                // Add here the name of scenarios to ignore and the reason, e.g.:
                { "g_V_group_byXageX", IgnoreReason.NullKeysInMapNotSupported },
                { "g_V_hasXperson_name_markoX_bothXknowsX_groupCount_byXvaluesXnameX_foldX", IgnoreReason.ArrayKeysInMapNotAssertingInGherkin }
            };
        
        private static class Keywords
        {
            public const string Given = "GIVEN";
            public const string And = "AND";
            public const string But = "BUT";
            public const string When = "WHEN";
            public const string Then = "THEN";
        }

        public enum StepBlock
        {
            Given,
            When,
            Then
        }
        
        private static readonly IDictionary<StepBlock, Type> Attributes = new Dictionary<StepBlock, Type>
        {
            { StepBlock.Given, typeof(GivenAttribute) },
            { StepBlock.When, typeof(WhenAttribute) },
            { StepBlock.Then, typeof(ThenAttribute) }
        };
        
        private readonly ITestOutputHelper _output;

        public GherkinTestRunner(ITestOutputHelper output)
        {
            _output = output;
        }

        [Fact]
        public void RunGherkinBasedTests()
        {
            WriteOutput("Starting Gherkin-based tests");
            var stepDefinitionTypes = GetStepDefinitionTypes();
            var results = new List<ResultFeature>();
            foreach (var feature in GetFeatures())
            {
                var resultFeature = new ResultFeature(feature);
                results.Add(resultFeature);
                foreach (var scenario in feature.Children)
                {
                    var failedSteps = new Dictionary<Step, Exception>();
                    resultFeature.Scenarios[scenario] = failedSteps;
                    if (IgnoredScenarios.TryGetValue(scenario.Name, out var reason))
                    {
                        failedSteps.Add(scenario.Steps.First(), new IgnoreException(reason));
                        continue;
                    }
                    StepBlock? currentStep = null;
                    StepDefinition stepDefinition = null;
                    foreach (var step in scenario.Steps)
                    {
                        var previousStep = currentStep;
                        currentStep = GetStepBlock(currentStep, step.Keyword);
                        if (currentStep == StepBlock.Given && previousStep != StepBlock.Given)
                        {
                            stepDefinition?.Dispose();
                            stepDefinition = GetStepDefinitionInstance(stepDefinitionTypes, step.Text);
                        }
                        if (stepDefinition == null)
                        {
                            throw new NotSupportedException(
                                $"Step '{step.Text} not supported without a 'Given' step first");
                        }
                        var result = ExecuteStep(stepDefinition, currentStep.Value, step);
                        if (result != null)
                        {
                            failedSteps.Add(step, result);
                            // Stop processing scenario
                            break;
                        }
                    }
                }
            }
            OutputResults(results);
            WriteOutput("Finished Gherkin-based tests");
            ScenarioData.Shutdown();
        }

        private void WriteOutput(string line)
        {
#if DEBUG
            _output.WriteLine(line);
#else
            Console.WriteLine(line);
#endif
        }

        private void OutputResults(List<ResultFeature> results)
        {
            WriteOutput("Gherkin tests result");
            var identifier = 0;
            var failures = new List<Tuple<string, Exception>>();
            var totalScenarios = 0;
            var totalFailed = 0;
            var totalIgnored = 0;
            foreach (var resultFeature in results)
            {
                foreach (var resultScenario in resultFeature.Scenarios)
                {
                    totalScenarios++;
                    WriteOutput($"  Scenario: {resultScenario.Key.Name}");
                    foreach (var step in resultScenario.Key.Steps)
                    {
                        resultScenario.Value.TryGetValue(step, out var failure);
                        if (failure == null)
                        {
                            WriteOutput($"    {step.Keyword} {step.Text}");
                        }
                        else
                        {
                            if (failure is IgnoreException)
                            {
                                totalIgnored++;
                                WriteOutput($"    {++identifier}) {step.Keyword} {step.Text} (ignored)");
                            }
                            else
                            {
                                totalFailed++;
                                WriteOutput($"    {++identifier}) {step.Keyword} {step.Text} (failed)");
                            }
                            failures.Add(Tuple.Create(resultScenario.Key.Name, failure));
                        }
                    }
                }
            }
            if (totalFailed > 0)
            {
                WriteOutput("Failures" + (totalIgnored > 0 ? " and skipped scenarios" : "") + ":");
            }
            else if (totalIgnored > 0)
            {
                WriteOutput("Skipped scenarios:");
            }
            for (var index = 0; index < failures.Count; index++)
            {
                var failure = failures[index];
                var message = failure.Item2 is IgnoreException
                    ? ": " + failure.Item2.Message
                    : ": Failed\n" + failure.Item2;
                WriteOutput($"{index+1}) {failure.Item1}{message}");
            }
            WriteOutput("-----------------");
            WriteOutput($"Total scenarios: {totalScenarios}." +
                              $" Passed: {totalScenarios-totalFailed-totalIgnored}." +
                              $" Failed: {totalFailed}. Skipped: {totalIgnored}.");
            if (totalFailed == 0)
            {
                return;
            }
            throw new Exception($"Gherkin test failed, see summary above for more detail");
        }

        public class ResultFeature
        {
            public Feature Feature { get;}

            public IDictionary<ScenarioDefinition, IDictionary<Step, Exception>> Scenarios { get; }

            public ResultFeature(Feature feature)
            {
                Feature = feature;
                Scenarios = new Dictionary<ScenarioDefinition, IDictionary<Step, Exception>>();
            }
        }

        private Exception ExecuteStep(StepDefinition instance, StepBlock stepBlock, Step step)
        {
            var attribute = Attributes[stepBlock];
            var methodAndParameters = instance.GetType().GetMethods()
                .Select(m =>
                {
                    var attr = (BddAttribute) m.GetCustomAttribute(attribute);
                    
                    if (attr == null)
                    {
                        return null;
                    }
                    var match = Regex.Match(step.Text, attr.Message);
                    if (!match.Success)
                    {
                        return null;
                    }
                    var parameters = new List<object>();
                    for (var i = 1; i < match.Groups.Count; i++)
                    {
                        parameters.Add(match.Groups[i].Value);
                    }
                    if (step.Argument is DocString)
                    {
                        parameters.Add(((DocString) step.Argument).Content);
                    }
                    else if (step.Argument != null)
                    {
                        parameters.Add(step.Argument);
                    }
                    var methodParameters = m.GetParameters();
                    for (var i = parameters.Count; i < methodParameters.Length; i++)
                    {
                        // Try to complete with default parameter values
                        var paramInfo = methodParameters[i];
                        if (!paramInfo.HasDefaultValue)
                        {
                            break;
                        }
                        parameters.Add(paramInfo.DefaultValue);
                    }
                    if (methodParameters.Length != parameters.Count)
                    {
                        return null;
                    }
                    return Tuple.Create(m, parameters.ToArray());
                })
                .FirstOrDefault(t => t != null);
            if (methodAndParameters == null)
            {
                throw new InvalidOperationException(
                    $"There is no step definition method for {stepBlock} '{step.Text}'");
            }
            try
            {
                var method = methodAndParameters.Item1;
                var parameters = methodAndParameters.Item2;
                var parameterInfos = method.GetParameters();
                for (var i = 0; i < parameterInfos.Length; i++)
                {
                    var paramInfo = parameterInfos[i];
                    // Do some minimal conversion => regex capturing groups to int
                    if (paramInfo.ParameterType == typeof(int))
                    {
                        parameters[i] = Convert.ToInt32(parameters[i]);
                    }
                }
                method.Invoke(instance, parameters);
            }
            catch (TargetInvocationException ex)
            {
                // Exceptions should not be thrown
                // Should be captured for result
                return ex.InnerException;
            }
            catch (Exception ex)
            {
                return ex;
            }
            // Success
            return null;
        }

        private static StepBlock GetStepBlock(StepBlock? currentStep, string stepKeyword)
        {
            switch (stepKeyword.Trim().ToUpper())
            {
                case Keywords.Given:
                    return StepBlock.Given;
                case Keywords.When:
                    return StepBlock.When;
                case Keywords.Then:
                    return StepBlock.Then;
                case Keywords.And:
                case Keywords.But:
                    if (currentStep == null)
                    {
                        throw new InvalidOperationException("'And' or 'But' is not supported outside a step");
                    }
                    return currentStep.Value;
            }
            throw new NotSupportedException($"Step with keyword {stepKeyword} not supported");
        }

        private static StepDefinition GetStepDefinitionInstance(IEnumerable<Type> stepDefinitionTypes, string stepText)
        {
            var type = stepDefinitionTypes
                .FirstOrDefault(t => t.GetMethods().Any(m =>
                {
                    var attr = m.GetCustomAttribute<GivenAttribute>();
                    if (attr == null)
                    {
                        return false;
                    }
                    return Regex.IsMatch(stepText, attr.Message);
                }));
            if (type == null)
            {
                throw new InvalidOperationException($"No step definition class matches Given '{stepText}'");
            }
            return (StepDefinition) Activator.CreateInstance(type);
        }

        private ICollection<Type> GetStepDefinitionTypes()
        {
            var assembly = GetType().GetTypeInfo().Assembly;
            var types = assembly.GetTypes()
                .Where(t => typeof(StepDefinition).IsAssignableFrom(t) && !t.GetTypeInfo().IsAbstract)
                .ToArray();
            if (types.Length == 0)
            {
                throw new InvalidOperationException($"No step definitions in {assembly.FullName}");
            }
            return types;
        }

        private IEnumerable<Feature> GetFeatures()
        {
            var rootPath = GetRootPath();
            var path = Path.Combine(rootPath, "gremlin-test", "features");
            var files = Directory.GetFiles(path, "*.feature", SearchOption.AllDirectories);
            foreach (var gherkinFile in files)
            {
                var parser = new Parser();
                var doc = parser.Parse(gherkinFile);
                yield return doc.Feature;
            }
        }

        private string GetRootPath()
        {
            var codeBaseUrl = new Uri(GetType().GetTypeInfo().Assembly.CodeBase);
            var codeBasePath = Uri.UnescapeDataString(codeBaseUrl.AbsolutePath);
            DirectoryInfo rootDir = null;
            for (var dir = Directory.GetParent(Path.GetDirectoryName(codeBasePath));
                dir.Parent != null;
                dir = dir.Parent)
            {
                if (dir.Name == "gremlin-dotnet" && dir.GetFiles("pom.xml").Length == 1)
                {
                    rootDir = dir.Parent;
                    break;
                }
            }
            if (rootDir == null)
            {
                throw new FileNotFoundException("tinkerpop root not found in path");
            }
            return rootDir.FullName;
        }
    }
}
