# Gremlin Grammar Generation

The Dart lexer/parser in this directory is generated from:

- `gremlin-language/src/main/antlr4/Gremlin.g4`

TinkerPop uses ANTLR `4.13.2`, and the Dart runtime dependency is pinned to the
matching `antlr4` package version in `gremlin-dart/pubspec.yaml`.

## Regenerate With the ANTLR Tool

From the repository root:

```bash
java -jar /path/to/antlr-4.13.2-complete.jar \
  -Dlanguage=Dart \
  -visitor \
  -no-listener \
  -Xexact-output-dir \
  -o gremlin-dart/lib/language/grammar \
  gremlin-language/src/main/antlr4/Gremlin.g4
```

This produces:

- `GremlinLexer.dart`
- `GremlinParser.dart`
- `GremlinVisitor.dart`
- `GremlinBaseVisitor.dart`

The accompanying `.interp` and `.tokens` files are also generated.

## Builder Option

If you prefer a Dart-native workflow, `antlr4_builder` can also drive
regeneration, but the generated output must remain equivalent to the command
above and continue to target ANTLR `4.13.2`.
