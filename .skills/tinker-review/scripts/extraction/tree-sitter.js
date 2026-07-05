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
import { readdir } from "node:fs/promises";
import { readFileSync } from "node:fs";
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

// Reduce a type-reference node (possibly generic or dotted) to its simple name,
// mirroring how calls resolve by simple name: `List<Foo>` -> "List",
// `a.b.Bar` -> "Bar". Returns null for shapes we can't name.
function baseTypeName(node) {
  if (!node) return null;
  if (node.type === "type_identifier" || node.type === "identifier") return node.text;
  if (node.type === "scoped_type_identifier" || node.type === "qualified_name") {
    let last = null;
    for (let i = 0; i < node.childCount; i++) {
      const c = node.child(i);
      if (c.type === "type_identifier" || c.type === "identifier") last = c;
    }
    return last ? last.text : null;
  }
  if (node.type === "generic_type" || node.type === "generic_name") {
    for (let i = 0; i < node.childCount; i++) {
      const c = node.child(i);
      const n = baseTypeName(c);
      if (n) return n;
    }
  }
  return null;
}

const TYPE_REF_NODES = new Set([
  "type_identifier", "scoped_type_identifier", "generic_type",
  "identifier", "qualified_name", "generic_name",
]);

// Collect the simple names of every type reference directly under a container
// (e.g. a `super_interfaces`/`base_list`/`type_list` node).
function typeNamesInContainer(container) {
  if (!container) return [];
  const list = childByType(container, "type_list") || container;
  const names = [];
  for (let i = 0; i < list.childCount; i++) {
    const child = list.child(i);
    if (TYPE_REF_NODES.has(child.type)) {
      const n = baseTypeName(child);
      if (n) names.push(n);
    }
  }
  return names;
}

// Extract a type's declared supertypes as {name, relation} pairs, where relation
// is "extends" (superclass / interface-extends-interface) or "implements"
// (class-implements-interface). Java is split precisely; other languages emit a
// best-effort "extends" for every base since their grammars don't cleanly
// separate the two. Go has no inheritance, so it yields nothing.
function extractSupertypes(node, language) {
  const supers = [];
  const push = (names, relation) => {
    for (const name of names) supers.push({ name, relation });
  };

  if (language === "java") {
    push(typeNamesInContainer(childByType(node, "superclass")), "extends");
    push(typeNamesInContainer(childByType(node, "super_interfaces")), "implements");
    // `interface A extends B` — an interface extending interfaces
    push(typeNamesInContainer(childByType(node, "extends_interfaces")), "extends");
    return supers;
  }

  if (language === "csharp") {
    // `class C : Base, IFoo` — base_list mixes the superclass and interfaces
    // with no grammatical distinction, so label them all "extends".
    push(typeNamesInContainer(childByType(node, "base_list")), "extends");
    return supers;
  }

  if (language === "javascript") {
    const heritage = childByType(node, "class_heritage");
    if (heritage) {
      const id = deepChildByType(heritage, "identifier");
      if (id) supers.push({ name: id.text, relation: "extends" });
    }
    return supers;
  }

  if (language === "python") {
    const args = childByType(node, "argument_list");
    if (args) push(typeNamesInContainer(args), "extends");
    return supers;
  }

  if (language === "dart") {
    const sup = childByType(node, "superclass");
    if (sup) push(typeNamesInContainer(sup), "extends");
    const interfaces = childByType(node, "interfaces");
    if (interfaces) push(typeNamesInContainer(interfaces), "implements");
    return supers;
  }

  return supers;
}

function extractTypesFromTree(tree, filePath, language, fileChanged) {
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
          changed: fileChanged,
          supertypes: extractSupertypes(node, language),
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

// Which type declares each function, for `declares` (Type -> Function) edges.
// Walks the tree carrying the innermost enclosing type so a method maps to the
// class/interface whose body it sits in (both keyed within the same file).
function extractDeclaresFromTree(tree, filePath, language) {
  const declares = [];

  function visit(node, enclosingType) {
    let currentType = enclosingType;
    if (isTypeNode(node, language)) {
      currentType = extractTypeName(node, language) || enclosingType;
    } else if (isFunctionNode(node, language) && enclosingType) {
      const fname = extractFunctionName(node, language);
      if (fname) declares.push({ typeName: enclosingType, functionName: fname, filePath });
    }
    for (let i = 0; i < node.childCount; i++) {
      visit(node.child(i), currentType);
    }
  }

  visit(tree.rootNode, null);
  return declares;
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

// Parse one source file and append everything it yields to `result`. Shared by
// the changed-file pass and the hierarchy-neighborhood pass; `file.changed`
// flows onto every function/type so context files land as changed:false.
function parseSourceFile(parser, file, language, result) {
  let content;
  try {
    content = readFileSync(file.fullPath, "utf-8");
  } catch (err) {
    // A file in the PR's changed set that isn't on disk was deleted by the PR —
    // there's no source to extract, so skip it rather than crash.
    if (err.code === "ENOENT") return;
    throw err;
  }
  const tree = parser.parse(content);
  if (!tree) return;

  result.files.push({ path: file.path, language, changed: file.changed });

  const fileFunctions = extractFunctionsFromTree(tree, file.path, language, file.changed);
  result.functions.push(...fileFunctions);
  result.types.push(...extractTypesFromTree(tree, file.path, language, file.changed));
  result.declares.push(...extractDeclaresFromTree(tree, file.path, language));

  const fileCalls = extractCallsFromTree(tree, file.path, language, fileFunctions);
  result.calls.push(...fileCalls);
  result.imports.push(...extractImportsFromTree(tree, file.path, language));

  if (isTestFile(file.path, language)) {
    for (const fn of fileFunctions) {
      result.tests.push({
        name: fn.name,
        type: classifyTestType(file.path, language),
        filePath: file.path,
        calledFunctions: fileCalls.filter((c) => c.callerName === fn.name).map((c) => c.calleeName),
      });
    }
  }

  tree.delete();
}

// Approximate, regex-based scan of one file's text for type declarations and the
// type names they reference in a supertype position. Deliberately NOT tree-sitter:
// this only SELECTS which files the neighborhood pass should accurately parse, so
// over- or under-inclusion costs at most a few extra/missing parses — never a
// wrong edge. Go is skipped (no inheritance).
function scanTypeHeaders(text, language) {
  const decls = [];
  const refs = [];
  if (language === "go") return { decls, refs };

  const declRe = /\b(?:class|interface|enum|struct|trait|record)\s+([A-Za-z_]\w*)/g;
  let m;
  while ((m = declRe.exec(text)) !== null) {
    decls.push(m[1]);
    // Header = from the declared name to the body opener (`{`), bounded so a
    // brace-less declaration (Python) can't swallow the whole file.
    const rest = text.slice(m.index + m[0].length);
    const brace = rest.indexOf("{");
    const nl = rest.indexOf("\n");
    let end = brace === -1 ? rest.length : brace;
    if (language === "python" && nl !== -1) end = Math.min(end, nl);
    end = Math.min(end, 300);
    for (const r of rest.slice(0, end).match(/\b[A-Z]\w*/g) || []) {
      if (r !== m[1]) refs.push(r);
    }
  }
  return { decls, refs };
}

/**
 * Find the type-hierarchy neighborhood of the changed types on disk: files that
 * must also be parsed so override/hierarchy edges don't undercount. Two bounded
 * BFS walks over a cheap regex index of the worktree:
 *
 *   - Downward: files declaring a type that extends/implements a changed type
 *     (transitively) — the overriders an interface change actually affects.
 *   - Upward: files declaring a changed type's supertypes, to the root — so a
 *     changed subclass resolves its overrides against real ancestor methods
 *     instead of empty external stubs.
 *
 * Returns relative paths to parse as context (changed:false), capped so a change
 * to a very central type can't drag in the whole module.
 */
async function hierarchyNeighborhood(directory, extensions, language, changedTypes, changedPaths, opts = {}) {
  const maxDepth = opts.maxDepth ?? 6;
  const maxFiles = opts.maxFiles ?? 250;

  const all = await walkDirectory(directory, extensions, new Set());
  const index = [];
  for (const f of all) {
    if (changedPaths.has(f.path)) continue;
    let text;
    try { text = readFileSync(f.fullPath, "utf-8"); } catch { continue; }
    const { decls, refs } = scanTypeHeaders(text, language);
    if (decls.length === 0) continue;
    index.push({ path: f.path, fullPath: f.fullPath, decls: new Set(decls), refs: new Set(refs) });
  }

  const included = new Set();
  const include = (entry) => {
    included.add(entry.path);
    return included.size >= maxFiles;
  };

  // Downward: who subtypes a frontier type.
  let downFrontier = new Set(changedTypes.map((t) => t.name));
  const downSeen = new Set(downFrontier);
  for (let d = 0; d < maxDepth && downFrontier.size; d++) {
    const next = new Set();
    for (const entry of index) {
      if (included.has(entry.path)) continue;
      let hit = false;
      for (const r of entry.refs) if (downFrontier.has(r)) { hit = true; break; }
      if (!hit) continue;
      if (include(entry)) return { files: [...included], truncated: true };
      for (const dn of entry.decls) if (!downSeen.has(dn)) { downSeen.add(dn); next.add(dn); }
    }
    downFrontier = next;
  }

  // Upward: declarers of a frontier supertype, following their own supertypes up.
  let upFrontier = new Set();
  for (const t of changedTypes) for (const s of (t.supertypes || [])) upFrontier.add(s.name);
  const upSeen = new Set(upFrontier);
  for (let d = 0; d < maxDepth && upFrontier.size; d++) {
    const next = new Set();
    for (const entry of index) {
      if (included.has(entry.path)) continue;
      let hit = false;
      for (const dn of entry.decls) if (upFrontier.has(dn)) { hit = true; break; }
      if (!hit) continue;
      if (include(entry)) return { files: [...included], truncated: true };
      for (const r of entry.refs) if (!upSeen.has(r)) { upSeen.add(r); next.add(r); }
    }
    upFrontier = next;
  }

  return { files: [...included], truncated: false };
}

/**
 * Parse source files in a directory using Tree-sitter.
 * Returns structured extraction data for graph population.
 *
 * @param {string} directory - Absolute path to the PR worktree
 * @param {string} language - Primary language to parse (e.g., "dart")
 * @param {object} options
 * @param {string[]} [options.changedFiles] - List of files changed in PR (relative paths)
 * @param {boolean} [options.expandHierarchy] - Pull in the type-hierarchy
 *   neighborhood of changed types so override/hierarchy edges don't undercount
 *   (default: true, only applies in changed-files mode)
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
    declares: [],
  };

  for (const file of sourceFiles) {
    parseSourceFile(parser, file, language, result);
  }

  // Hierarchy-neighborhood expansion: in changed-files mode the graph only holds
  // PR-touched files, so a changed interface's overriders (and a changed
  // subclass's ancestors) live in unparsed files — override/hierarchy edges
  // would undercount. Pull those neighborhood files in as context (changed:false)
  // so the edges resolve against real vertices instead of empty external stubs.
  const expandHierarchy = options.expandHierarchy !== false;
  if (expandHierarchy && changedFiles.length > 0 && result.types.length > 0) {
    const changedPaths = new Set(sourceFiles.map((f) => f.path));
    const changedTypes = result.types.slice();
    const { files: extraPaths, truncated } = await hierarchyNeighborhood(
      directory, extensions, language, changedTypes, changedPaths,
    );
    for (const relPath of extraPaths) {
      parseSourceFile(parser, { path: relPath, fullPath: join(directory, relPath), changed: false }, language, result);
    }
    result.hierarchyNeighborhood = { files: extraPaths.length, truncated };
  }

  parser.delete();
  return result;
}
