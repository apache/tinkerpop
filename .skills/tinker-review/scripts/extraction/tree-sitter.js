/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import { createRequire } from "node:module";
const require = createRequire(import.meta.url);
const TreeSitter = require("web-tree-sitter");
import { readdir, readFile } from "node:fs/promises";
import { join, relative } from "node:path";
import { fileURLToPath } from "node:url";
import { dirname } from "node:path";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const LANGUAGE_EXTENSIONS = {
  java: [".java"],
  javascript: [".js", ".mjs", ".cjs"],
  go: [".go"],
  python: [".py"],
  csharp: [".cs"],
  dart: [".dart"],
};

const WASM_FILENAMES = {
  java: "tree-sitter-java.wasm",
  javascript: "tree-sitter-javascript.wasm",
  go: "tree-sitter-go.wasm",
  python: "tree-sitter-python.wasm",
  csharp: "tree-sitter-c_sharp.wasm",
  dart: "tree-sitter-dart.wasm",
};

const SKIP_DIRS = new Set([
  "node_modules", "build", "target", ".git", ".gradle",
  "dist", "out", "__pycache__", ".dart_tool", ".pub-cache",
  "vendor", "bin", "obj",
]);

let initialized = false;

async function ensureInit() {
  if (!initialized) {
    const wasmPath = join(__dirname, "..", "..", "node_modules", "web-tree-sitter", "tree-sitter.wasm");
    await TreeSitter.init({ locateFile: () => wasmPath });
    initialized = true;
  }
}

async function loadLanguage(language) {
  const wasmFile = WASM_FILENAMES[language];
  if (!wasmFile) {
    throw new Error(`Unsupported language: ${language}. Supported: ${Object.keys(WASM_FILENAMES).join(", ")}`);
  }
  const wasmPath = join(__dirname, "..", "..", "node_modules", "tree-sitter-wasms", "out", wasmFile);
  return TreeSitter.Language.load(wasmPath);
}

async function walkDirectory(directory, extensions, changedSet) {
  const files = [];

  async function walk(dir) {
    let entries;
    try {
      entries = await readdir(dir, { withFileTypes: true });
    } catch {
      return;
    }
    for (const entry of entries) {
      if (entry.name.startsWith(".") || SKIP_DIRS.has(entry.name)) {
        continue;
      }
      const fullPath = join(dir, entry.name);
      if (entry.isDirectory()) {
        await walk(fullPath);
      } else if (extensions.some((ext) => entry.name.endsWith(ext))) {
        const relPath = relative(directory, fullPath);
        files.push({
          path: relPath,
          fullPath,
          changed: changedSet.has(relPath),
        });
      }
    }
  }

  await walk(directory);
  return files;
}

function isFunctionNode(node, language) {
  const types = {
    java: ["method_declaration", "constructor_declaration"],
    go: ["function_declaration", "method_declaration"],
    python: ["function_definition"],
    javascript: ["function_declaration", "method_definition"],
    csharp: ["method_declaration", "constructor_declaration"],
    dart: ["method_signature", "function_signature"],
  };
  return (types[language] || []).includes(node.type);
}

function isTypeNode(node, language) {
  const types = {
    java: ["class_declaration", "interface_declaration", "enum_declaration"],
    go: ["type_declaration"],
    python: ["class_definition"],
    javascript: ["class_declaration"],
    csharp: ["class_declaration", "interface_declaration", "struct_declaration", "enum_declaration"],
    dart: ["class_definition", "enum_declaration"],
  };
  return (types[language] || []).includes(node.type);
}

function isCallNode(node, language) {
  const types = {
    java: ["method_invocation"],
    go: ["call_expression"],
    python: ["call"],
    javascript: ["call_expression"],
    csharp: ["invocation_expression"],
    dart: ["call_expression"],
  };
  return (types[language] || []).includes(node.type);
}

function isImportNode(node, language) {
  const types = {
    java: ["import_declaration"],
    go: ["import_spec"],
    python: ["import_statement", "import_from_statement"],
    javascript: ["import_statement"],
    csharp: ["using_directive"],
    dart: ["import_or_export"],
  };
  return (types[language] || []).includes(node.type);
}

function childByType(node, type) {
  for (let i = 0; i < node.childCount; i++) {
    const child = node.child(i);
    if (child.type === type) return child;
  }
  return null;
}

function childrenByType(node, type) {
  const results = [];
  for (let i = 0; i < node.childCount; i++) {
    const child = node.child(i);
    if (child.type === type) results.push(child);
  }
  return results;
}

function deepChildByType(node, type) {
  if (node.type === type) return node;
  for (let i = 0; i < node.childCount; i++) {
    const result = deepChildByType(node.child(i), type);
    if (result) return result;
  }
  return null;
}

function getVisibility(node, language) {
  const modNode = childByType(node, "modifiers");
  if (modNode) {
    const text = modNode.text;
    if (text.includes("private")) return "private";
    if (text.includes("protected")) return "protected";
    if (text.includes("internal")) return "internal";
    return "public";
  }

  if (language === "go") {
    const nameNode = childByType(node, "identifier") || childByType(node, "field_identifier") || childByType(node, "type_identifier");
    if (nameNode) {
      const name = nameNode.text;
      return name[0] === name[0].toUpperCase() ? "public" : "private";
    }
    return "public";
  }

  if (language === "python") {
    const nameNode = childByType(node, "identifier");
    if (nameNode) {
      const name = nameNode.text;
      if (name.startsWith("__") && !name.endsWith("__")) return "private";
      if (name.startsWith("_")) return "protected";
    }
    return "public";
  }

  return "public";
}

function extractFunctionName(node, language) {
  if (language === "go" && node.type === "method_declaration") {
    const fieldId = childByType(node, "field_identifier");
    if (fieldId) return fieldId.text;
  }

  const nameNode = childByType(node, "identifier");
  if (nameNode) return nameNode.text;

  if (language === "javascript" && node.type === "method_definition") {
    const propId = childByType(node, "property_identifier");
    if (propId) return propId.text;
  }

  return null;
}

function extractSignature(node, language) {
  const name = extractFunctionName(node, language);
  if (!name) return null;

  let params = "";
  const paramNode = childByType(node, "formal_parameters")
    || childByType(node, "parameter_list")
    || childByType(node, "parameters")
    || childByType(node, "formal_parameter_list");
  if (paramNode) params = paramNode.text;

  if (language === "go" && node.type === "method_declaration") {
    const paramLists = childrenByType(node, "parameter_list");
    const receiver = paramLists.length > 0 ? paramLists[0].text : "";
    const funcParams = paramLists.length > 1 ? paramLists[1].text : "";
    return `(${receiver}) ${name}${funcParams}`;
  }

  return `${name}${params}`;
}

function extractFunctionsFromTree(tree, filePath, language, fileChanged) {
  const functions = [];

  function visit(node) {
    if (isFunctionNode(node, language)) {
      const name = extractFunctionName(node, language);
      if (name) {
        functions.push({
          name,
          signature: extractSignature(node, language) || name,
          visibility: getVisibility(node, language),
          filePath,
          linesStart: node.startPosition.row + 1,
          linesEnd: node.endPosition.row + 1,
          changed: fileChanged,
        });
      }
    }
    for (let i = 0; i < node.childCount; i++) {
      visit(node.child(i));
    }
  }

  visit(tree.rootNode);
  return functions;
}

function inferTypeKind(node, language) {
  const t = node.type;
  if (t.includes("interface")) return "interface";
  if (t.includes("struct")) return "struct";
  if (t.includes("enum")) return "enum";
  if (language === "go" && t === "type_declaration") {
    const spec = childByType(node, "type_spec");
    if (spec) {
      for (let i = 0; i < spec.childCount; i++) {
        const child = spec.child(i);
        if (child.type === "interface_type") return "interface";
        if (child.type === "struct_type") return "struct";
      }
    }
  }
  return "class";
}

function extractTypeName(node, language) {
  if (language === "go" && node.type === "type_declaration") {
    const spec = childByType(node, "type_spec");
    if (spec) {
      const typeId = childByType(spec, "type_identifier");
      if (typeId) return typeId.text;
    }
    return null;
  }
  const nameNode = childByType(node, "identifier");
  return nameNode ? nameNode.text : null;
}

function extractTypesFromTree(tree, filePath, language) {
  const types = [];

  function visit(node) {
    if (isTypeNode(node, language)) {
      const name = extractTypeName(node, language);
      if (name) {
        types.push({
          name,
          kind: inferTypeKind(node, language),
          visibility: getVisibility(node, language),
          filePath,
        });
      }
    }
    for (let i = 0; i < node.childCount; i++) {
      visit(node.child(i));
    }
  }

  visit(tree.rootNode);
  return types;
}

function extractCalleeName(node, language) {
  if (language === "java") {
    const nameNode = childByType(node, "identifier");
    return nameNode ? nameNode.text : null;
  }

  if (language === "go") {
    const firstChild = node.child(0);
    if (!firstChild) return null;
    if (firstChild.type === "identifier") return firstChild.text;
    if (firstChild.type === "selector_expression") {
      const field = childByType(firstChild, "field_identifier");
      return field ? field.text : null;
    }
    return null;
  }

  if (language === "python") {
    const firstChild = node.child(0);
    if (!firstChild) return null;
    if (firstChild.type === "identifier") return firstChild.text;
    if (firstChild.type === "attribute") {
      const attr = childByType(firstChild, "identifier");
      return attr ? attr.text : null;
    }
    return null;
  }

  if (language === "javascript" || language === "csharp" || language === "dart") {
    const firstChild = node.child(0);
    if (!firstChild) return null;
    if (firstChild.type === "identifier") return firstChild.text;
    if (firstChild.type === "member_expression" || firstChild.type === "member_access_expression") {
      const prop = childByType(firstChild, "property_identifier") || childByType(firstChild, "identifier");
      return prop ? prop.text : null;
    }
    const id = deepChildByType(firstChild, "identifier");
    return id ? id.text : null;
  }

  return null;
}

function findEnclosingFunction(line, functionRanges) {
  for (const fn of functionRanges) {
    if (line >= fn.linesStart && line <= fn.linesEnd) {
      return fn.name;
    }
  }
  return null;
}

function extractCallsFromTree(tree, filePath, language, functionRanges) {
  const calls = [];

  function visit(node) {
    if (isCallNode(node, language)) {
      const calleeName = extractCalleeName(node, language);
      if (calleeName) {
        const line = node.startPosition.row + 1;
        calls.push({
          callerName: findEnclosingFunction(line, functionRanges) || "<module>",
          callerFile: filePath,
          calleeName,
          line,
        });
      }
    }
    for (let i = 0; i < node.childCount; i++) {
      visit(node.child(i));
    }
  }

  visit(tree.rootNode);
  return calls;
}

function extractImportInfo(node, filePath, language) {
  if (language === "java") {
    const scopedId = childByType(node, "scoped_identifier");
    if (!scopedId) return null;
    const path = scopedId.text;
    const parts = path.split(".");
    return { filePath, importedPath: path, importedName: parts[parts.length - 1] };
  }

  if (language === "go") {
    const pathNode = childByType(node, "interpreted_string_literal");
    if (!pathNode) return null;
    const path = pathNode.text.replace(/"/g, "");
    const parts = path.split("/");
    return { filePath, importedPath: path, importedName: parts[parts.length - 1] };
  }

  if (language === "python") {
    if (node.type === "import_from_statement") {
      const dotted = childByType(node, "dotted_name");
      if (!dotted) return null;
      return { filePath, importedPath: dotted.text, importedName: "*" };
    }
    const dotted = childByType(node, "dotted_name");
    if (!dotted) return null;
    return { filePath, importedPath: dotted.text, importedName: dotted.text };
  }

  if (language === "javascript") {
    const srcNode = childByType(node, "string");
    if (!srcNode) return null;
    const path = srcNode.text.replace(/['"]/g, "");
    return { filePath, importedPath: path, importedName: "*" };
  }

  if (language === "csharp") {
    const qName = childByType(node, "qualified_name") || childByType(node, "identifier");
    if (!qName) return null;
    const path = qName.text;
    const parts = path.split(".");
    return { filePath, importedPath: path, importedName: parts[parts.length - 1] };
  }

  if (language === "dart") {
    const strLit = deepChildByType(node, "string_literal");
    if (!strLit) return null;
    const path = strLit.text.replace(/['"]/g, "");
    const parts = path.split("/");
    const name = parts[parts.length - 1].replace(".dart", "");
    return { filePath, importedPath: path, importedName: name };
  }

  return null;
}

function extractImportsFromTree(tree, filePath, language) {
  const imports = [];

  function visit(node) {
    if (isImportNode(node, language)) {
      const info = extractImportInfo(node, filePath, language);
      if (info) imports.push(info);
    }
    for (let i = 0; i < node.childCount; i++) {
      visit(node.child(i));
    }
  }

  visit(tree.rootNode);
  return imports;
}

function isTestFile(filePath, language) {
  switch (language) {
    case "java":
      return filePath.includes("src/test/") || filePath.endsWith("Test.java");
    case "go":
      return filePath.endsWith("_test.go");
    case "python":
      return /(?:^|\/|\\)test_[^/\\]*\.py$/.test(filePath);
    case "dart":
      return filePath.includes("_test.dart") || filePath.includes("/test/");
    case "javascript":
      return filePath.includes(".test.") || filePath.includes(".spec.") || filePath.includes("/test/");
    case "csharp":
      return filePath.includes("Test") && filePath.includes("/Tests/");
    default:
      return false;
  }
}

function classifyTestType(filePath, language) {
  if (language === "java" && filePath.includes("src/test/")) {
    if (filePath.toLowerCase().includes("integration")) return "integration";
    return "unit";
  }
  return "unit";
}

/**
 * Parse source files in a directory using Tree-sitter.
 * Returns structured extraction data for graph population.
 *
 * @param {string} directory - Absolute path to the PR worktree
 * @param {string} language - Primary language to parse (e.g., "dart")
 * @param {object} options
 * @param {string[]} [options.changedFiles] - List of files changed in PR (relative paths)
 * @returns {Promise<ExtractionResult>}
 */
export async function extract(directory, language, options = {}) {
  const changedFiles = options.changedFiles || [];
  const changedSet = new Set(changedFiles);

  const extensions = LANGUAGE_EXTENSIONS[language];
  if (!extensions) {
    throw new Error(`Unsupported language: ${language}. Supported: ${Object.keys(LANGUAGE_EXTENSIONS).join(", ")}`);
  }

  await ensureInit();
  const lang = await loadLanguage(language);
  const parser = new TreeSitter();
  parser.setLanguage(lang);

  let sourceFiles;
  if (changedFiles.length > 0) {
    sourceFiles = changedFiles
      .filter(f => extensions.some(ext => f.endsWith(ext)))
      .map(f => ({ path: f, fullPath: join(directory, f), changed: true }));
  } else {
    sourceFiles = await walkDirectory(directory, extensions, changedSet);
  }

  const result = {
    language,
    files: [],
    functions: [],
    types: [],
    calls: [],
    imports: [],
    tests: [],
  };

  for (const file of sourceFiles) {
    let content;
    try {
      content = await readFile(file.fullPath, "utf-8");
    } catch (err) {
      // A file in the PR's changed set that isn't on disk was deleted by the
      // PR — there's no source to extract, so skip it rather than crash.
      if (err.code === "ENOENT") continue;
      throw err;
    }
    const tree = parser.parse(content);
    if (!tree) continue;

    result.files.push({
      path: file.path,
      language,
      changed: file.changed,
    });

    const fileFunctions = extractFunctionsFromTree(tree, file.path, language, file.changed);
    result.functions.push(...fileFunctions);

    const fileTypes = extractTypesFromTree(tree, file.path, language);
    result.types.push(...fileTypes);

    const fileCalls = extractCallsFromTree(tree, file.path, language, fileFunctions);
    result.calls.push(...fileCalls);

    const fileImports = extractImportsFromTree(tree, file.path, language);
    result.imports.push(...fileImports);

    if (isTestFile(file.path, language)) {
      for (const fn of fileFunctions) {
        result.tests.push({
          name: fn.name,
          type: classifyTestType(file.path, language),
          filePath: file.path,
          calledFunctions: fileCalls
            .filter((c) => c.callerName === fn.name)
            .map((c) => c.calleeName),
        });
      }
    }

    tree.delete();
  }

  parser.delete();
  return result;
}
