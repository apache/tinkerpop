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
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;
using Xunit;
using Gherkin;
using Gherkin.Ast;
using Gremlin.Net.IntegrationTest.Gherkin.Attributes;
using Microsoft.VisualStudio.TestPlatform.Utilities;
using Newtonsoft.Json.Serialization;
using Xunit.Abstractions;
using Xunit.Sdk;

namespace Gremlin.Net.IntegrationTest.Gherkin
{
    public class GherkinTestRunner
    {
        private readonly ITestOutputHelper _output;

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

        public GherkinTestRunner(ITestOutputHelper output)
        {
            _output = output;
        }

        [Fact]
        public void RunGherkinBasedTests()
        {
            Console.WriteLine("Starting Gherkin-based tests");
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
                        }
                    }
                }
            }
            OutputResults(results);
            Console.WriteLine("Finished Gherkin-based tests");
        }

        private void WriteOutput(string line)
        {
            _output.WriteLine(line);
        }

        private void OutputResults(List<ResultFeature> results)
        {
            var totalScenarios = results.Sum(f => f.Scenarios.Count);
            var totalFailedScenarios = results.Sum(f => f.Scenarios.Count(s => s.Value.Count > 0));
            WriteOutput("Gherkin tests summary");
            WriteOutput($"Total scenarios: {totalScenarios}. " +
                              $"Passed: {totalScenarios-totalFailedScenarios}. Failed: {totalFailedScenarios}.");
            if (totalFailedScenarios == 0)
            {
                return;
            }
            var identifier = 0;
            var failures = new List<Exception>();
            foreach (var resultFeature in results)
            {
                var failedScenarios = resultFeature.Scenarios.Where(s => s.Value.Count > 0).ToArray();
                if (failedScenarios.Length > 0)
                {
                    WriteOutput($"Feature: {resultFeature.Feature.Name}");   
                }
                else
                {
                    continue;
                }
                foreach (var resultScenario in failedScenarios)
                {
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
                            WriteOutput($"    {++identifier}) {step.Keyword} {step.Text} (failed)");
                            failures.Add(failure);
                        }
                    }
                }
            }
            WriteOutput("Failures:");
            for (var index = 0; index < failures.Count; index++)
            {
                WriteOutput($"{index+1}) {failures[index]}");
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
                    if (m.GetParameters().Length != parameters.Count)
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
                methodAndParameters.Item1.Invoke(instance, methodAndParameters.Item2);
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
            WriteOutput(path);
            WriteOutput("------");

            var files = new [] {"/Users/jorge/workspace/temp/count.feature"};
            //var files = Directory.GetFiles(path, "*.feature", SearchOption.AllDirectories);
            foreach (var gherkinFile in files)
            {
                var parser = new Parser();
                WriteOutput("Parsing " + gherkinFile);
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
                if (dir.Name == "gremlin-dotnet" && dir.Parent?.Name == "tinkerpop")
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

        private void PrintGherkin()
        {
            var gherkinFile = "/Users/jorge/workspace/temp/count.feature";
            var parser = new Parser();
            GherkinDocument doc = parser.Parse(gherkinFile);
            foreach (var scenario in doc.Feature.Children)
            {
                WriteOutput("--------");
                WriteOutput("Scenario: " + scenario.Name);
                foreach (var step in scenario.Steps)
                {
                    WriteOutput("  Step");
                    WriteOutput("    Keyword: " + step.Keyword);
                    WriteOutput("    Text: " + step.Text);
                    WriteOutput("    Argument: " + step.Argument);
                    if (step.Argument is DocString)
                    {
                        WriteOutput("      " + ((DocString)step.Argument).Content);
                    }
                    if (step.Argument is DataTable)
                    {
                        foreach (var row in ((DataTable)step.Argument).Rows)
                        {
                            WriteOutput("      Row: " + string.Join(", ", row.Cells.Select(x => x.Value)));   
                        }
                    }
                }
            }
        }
    }
}