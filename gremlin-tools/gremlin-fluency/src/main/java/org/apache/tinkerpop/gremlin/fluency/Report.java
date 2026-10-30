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
package org.apache.tinkerpop.gremlin.fluency;

import org.apache.tinkerpop.shaded.jackson.core.type.TypeReference;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeSet;

/**
 * Renders a {@link Snapshot} as a self-contained {@code report.html} with inline CSS and SVG and no external files,
 * so that it reads anywhere the snapshot is kept. Its one inline script only adds buttons that copy scenario names, and
 * the report reads the same without it. It leads with each context's difference from {@code none} within a model,
 * on the verified pass rate with the raw rate beside it, then each setup's results and tool use, breakdowns by graph
 * and feature file, and every scenario with the traversals each setup wrote and the tool calls behind them.
 * <p/>
 * Usage: {@code Report <snapshot.json or run directory>} writes {@code report.html} beside the snapshot.
 */
public final class Report {
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> MAP = new TypeReference<Map<String, Object>>() {};
    private static final String TEMPLATE = load();

    private static final String[] OUTCOMES = {"PASS", "WRONG_RESULT", "RUNTIME_ERROR", "PARSE_FAILURE", "TIMEOUT", "DECLINED", "NO_ANSWER"};
    private static final String[] OUTCOME_COLORS = {"--pass", "--wrong", "--runtime", "--parse", "--timeout", "--declined", "--none"};

    private Report() {}

    public static void main(final String[] args) throws IOException {
        if (args.length != 1) {
            System.err.println("Usage: Report <snapshot.json or run directory>");
            System.exit(1);
        }
        final Path path = Paths.get(args[0]);
        final Path snapshotFile = Files.isDirectory(path) ? path.resolve("snapshot.json") : path;
        final Path report = write(mapper.readValue(snapshotFile.toFile(), MAP), snapshotFile.toAbsolutePath().getParent());
        System.out.println("Wrote " + report.toAbsolutePath());
    }

    /**
     * Writes the report of the snapshot to {@code report.html} in the directory.
     */
    public static Path write(final Map<String, Object> snapshot, final Path directory) throws IOException {
        final Path report = directory.resolve("report.html");
        Files.write(report, render(snapshot).getBytes(StandardCharsets.UTF_8));
        return report;
    }

    static String render(final Map<String, Object> snapshot) {
        final List<Map<String, Object>> setups = records(snapshot.get("setups"));
        final StringBuilder html = new StringBuilder();
        header(html, snapshot);
        for (Map<String, Object> setup : setups) {
            final long attempts = number(setup.get("attempts")).longValue();
            if ("reference".equals(setup.get("driver")) && attempts > 0 &&
                    number(setup.get("verified")).longValue() < attempts) {
                html.append("<div class=\"banner\">The reference control <b>").append(escape(setup.get("name"))).
                        append("</b> verified ").append(setup.get("verified")).append(" of ").append(attempts).
                        append(" attempts. It answers with each scenario's own traversal, so the eval itself was not ")
                        .append("sound in this run and the results below should not be trusted.</div>\n");
            }
        }
        comparisons(html, records(snapshot.get("comparisons")));
        setups(html, setups);
        tools(html, setups);
        slices(html, setups, "byGraph", "By graph");
        slices(html, setups, "byFeature", "By feature file");
        scenarios(html, setups, records(snapshot.get("scenarios")));
        return TEMPLATE.replace("{{title}}", escape("Fluency run " + snapshot.get("runId"))).
                replace("{{body}}", html.toString());
    }

    private static void header(final StringBuilder html, final Map<String, Object> snapshot) {
        html.append("<h1>Gremlin fluency run ").append(escape(snapshot.get("runId"))).append("</h1>\n");
        html.append("<dl class=\"meta\">\n");
        final Map<String, Object> git = map(snapshot.get("git"));
        final Object sha = null == git ? null : git.get("sha");
        meta(html, "Commit", null == sha ? "unknown" : sha + (Boolean.TRUE.equals(git.get("dirty")) ? " (uncommitted changes)" : ""));
        meta(html, "Version", snapshot.get("version"));
        meta(html, "Corpus", snapshot.get("corpusHash"));
        meta(html, "Prompt", snapshot.get("promptHash"));
        meta(html, "Attempts per scenario", snapshot.get("attempts"));
        meta(html, "Differential seeds", snapshot.get("seeds"));
        html.append("</dl>\n");
    }

    private static void meta(final StringBuilder html, final String name, final Object value) {
        html.append("<dt>").append(name).append("</dt><dd><code>").append(escape(null == value ? "unknown" : value)).
                append("</code></dd>\n");
    }

    private static void comparisons(final StringBuilder html, final List<Map<String, Object>> comparisons) {
        html.append("<h2>Context against none</h2>\n");
        html.append("<p class=\"note\">Each setup's pass rate less that of the setup with no context for the same model ")
                .append("and harness, averaged over the scenarios both attempted, in percentage points. The interval is ")
                .append("a 95% bootstrap over scenarios and attempts. A difference whose interval spans zero is noise. ")
                .append("Absolute rates are not compared across models.</p>\n");
        if (comparisons.isEmpty()) {
            html.append("<p class=\"muted\">No comparisons: no setup shares a model and harness with a setup whose ")
                    .append("context is <code>none</code>.</p>\n");
            return;
        }
        html.append("<div class=\"scroll\"><table>\n<tr><th>Model</th><th>Harness</th><th>Setup</th><th>Context</th>")
                .append("<th>Baseline</th><th class=\"num\">Scenarios</th><th class=\"num\">Verified</th>")
                .append("<th>Interval</th><th class=\"num\">Raw</th></tr>\n");
        for (Map<String, Object> c : comparisons) {
            final Map<String, Object> verified = map(c.get("verified"));
            final Map<String, Object> raw = map(c.get("raw"));
            final List<Object> interval = list(verified.get("interval"));
            final double lo = number(interval.get(0)).doubleValue();
            final double hi = number(interval.get(1)).doubleValue();
            html.append("<tr><td>").append(escape(c.get("model"))).append("</td><td>").append(escape(c.get("harness")))
                    .append("</td><td>").append(escape(c.get("setup"))).append("</td><td>").append(escape(join(c.get("context"))))
                    .append("</td><td>").append(escape(c.get("baseline"))).append("</td><td class=\"num\">")
                    .append(c.get("scenarios")).append("</td><td class=\"num\">")
                    .append(points(number(verified.get("difference")).doubleValue())).append("<div class=\"sub\">")
                    .append(signed(lo)).append(" to ").append(signed(hi)).append("</div></td><td>")
                    .append(intervalSvg(number(verified.get("difference")).doubleValue(), lo, hi))
                    .append("</td><td class=\"num\">").append(points(number(raw.get("difference")).doubleValue()))
                    .append("</td></tr>\n");
        }
        html.append("</table></div>\n");
    }

    private static void setups(final StringBuilder html, final List<Map<String, Object>> setups) {
        html.append("<h2>Setups</h2>\n");
        html.append("<p class=\"note\">A pass is verified unless the differential check found that it only matched on ")
                .append("this data. Usage is per attempt that reported it.</p>\n");
        legend(html);
        html.append("<div class=\"scroll\"><table>\n<tr><th>Setup</th><th>Model</th><th>Harness</th><th>Context</th>")
                .append("<th class=\"num\">Attempts</th><th class=\"num\">Verified</th><th class=\"num\">Raw</th>")
                .append("<th>Outcomes</th><th class=\"num\">Turns</th><th class=\"num\">Tokens in / out</th></tr>\n");
        for (Map<String, Object> s : setups) {
            final Map<String, Object> usage = map(s.get("usage"));
            final Map<String, Object> perAttempt = null == usage ? null : map(usage.get("perAttempt"));
            html.append("<tr><td><b>").append(escape(s.get("name"))).append("</b>");
            if (s.get("version") != null)
                html.append("<div class=\"sub\">").append(escape(s.get("version"))).append("</div>");
            html.append("</td><td>").append(escape(orDash(s.get("model")))).append("</td><td>")
                    .append(escape(orDash(s.get("harness")))).append("</td><td>").append(escape(orDash(join(s.get("context")))))
                    .append("</td><td class=\"num\">").append(s.get("attempts")).append("</td><td class=\"num\">")
                    .append(rate(s.get("verifiedPassRate"))).append("</td><td class=\"num\">")
                    .append(percent(s.get("passRate"))).append("</td><td>")
                    .append(outcomesSvg(map(s.get("outcomes")), number(s.get("attempts")).longValue()))
                    .append("</td><td class=\"num\">").append(null == perAttempt ? "unknown" : decimal(perAttempt.get("turns")))
                    .append("</td><td class=\"num\">").append(null == perAttempt ? "unknown" :
                            whole(perAttempt.get("input_tokens")) + " / " + whole(perAttempt.get("output_tokens")))
                    .append("</td></tr>\n");
        }
        html.append("</table></div>\n");
    }

    private static void tools(final StringBuilder html, final List<Map<String, Object>> setups) {
        html.append("<h2>Tool use</h2>\n");
        if (setups.stream().noneMatch(s -> s.get("tools") != null)) {
            html.append("<p class=\"muted\">No setup reported the tools its agent called.</p>\n");
            return;
        }
        html.append("<p class=\"note\">For each setup whose driver reported its tool calls, the attempts that called ")
                .append("each tool, its calls, and the verified pass rate of those attempts, beside that of the attempts ")
                .append("that called no tool. An attempt that called several tools counts under each.</p>\n");
        html.append("<div class=\"scroll\"><table>\n<tr><th>Setup</th><th>Tool</th><th class=\"num\">Attempts</th>")
                .append("<th class=\"num\">Calls</th><th class=\"num\">Verified</th></tr>\n");
        for (Map<String, Object> s : setups) {
            final Map<String, Object> tools = map(s.get("tools"));
            if (null == tools) continue;
            final long reporting = number(tools.get("attemptsReporting")).longValue();
            // the setup is named once, on its first row
            String setup = "<b>" + escape(s.get("name")) + "</b><div class=\"sub\">" +
                    decimal(tools.get("callsPerAttempt")) + " calls per attempt</div>";
            for (Map.Entry<String, Object> tool : map(tools.get("byTool")).entrySet()) {
                toolRow(html, setup, "<code>" + escape(shortTool(tool.getKey())) + "</code>", map(tool.getValue()), reporting);
                setup = "";
            }
            toolRow(html, setup, "<span class=\"muted\">no tool</span>", map(tools.get("withoutTools")), reporting);
        }
        html.append("</table></div>\n");
    }

    private static void toolRow(final StringBuilder html, final String setup, final String tool,
                                final Map<String, Object> counts, final long reporting) {
        final long attempts = number(counts.get("attempts")).longValue();
        html.append("<tr><td>").append(setup).append("</td><td>").append(tool).append("</td><td class=\"num\">")
                .append(attempts).append("<div class=\"sub\">")
                .append(percent(reporting == 0 ? null : (double) attempts / reporting))
                .append("</div></td><td class=\"num\">").append(null == counts.get("calls") ? "–" : counts.get("calls"))
                .append("</td><td class=\"num\">")
                .append(attempts == 0 ? "–" : rate((double) number(counts.get("verified")).longValue() / attempts))
                .append("</td></tr>\n");
    }

    /**
     * A tool's name without the {@code mcp__<server>__} prefix Claude Code gives MCP tools.
     */
    static String shortTool(final String tool) {
        return tool.startsWith("mcp__") && tool.indexOf("__", 5) > 0 ? tool.substring(tool.indexOf("__", 5) + 2) : tool;
    }

    private static void slices(final StringBuilder html, final List<Map<String, Object>> setups, final String key,
                               final String title) {
        final TreeSet<String> names = new TreeSet<>();
        for (Map<String, Object> s : setups) {
            final Map<String, Object> slices = map(s.get(key));
            if (slices != null) names.addAll(slices.keySet());
        }
        if (names.isEmpty()) return;

        html.append("<h2>").append(title).append("</h2>\n<div class=\"scroll\"><table>\n<tr><th></th>");
        for (Map<String, Object> s : setups) {
            html.append("<th class=\"num\">").append(escape(s.get("name"))).append("</th>");
        }
        html.append("</tr>\n");
        for (String name : names) {
            html.append("<tr><td><code>").append(escape(name)).append("</code></td>");
            for (Map<String, Object> s : setups) {
                final Map<String, Object> slices = map(s.get(key));
                final Map<String, Object> slice = null == slices ? null : map(slices.get(name));
                html.append("<td class=\"num\">");
                if (slice != null) {
                    html.append(rate(slice.get("verifiedPassRate"))).append("<div class=\"sub\">raw ")
                            .append(percent(slice.get("passRate"))).append(" of ").append(slice.get("attempts")).append("</div>");
                }
                html.append("</td>");
            }
            html.append("</tr>\n");
        }
        html.append("</table></div>\n");
    }

    private static void scenarios(final StringBuilder html, final List<Map<String, Object>> setups,
                                  final List<Map<String, Object>> scenarios) {
        html.append("<h2>Scenarios</h2>\n");
        html.append("<p class=\"note\">Each cell is a setup's verified passes over its attempts. Open a scenario for ")
                .append("its intent, its reference traversal and every distinct traversal each setup wrote.</p>\n");
        html.append("<div class=\"scenario-head\"><span class=\"cells head\">");
        for (Map<String, Object> s : setups) {
            html.append("<span title=\"").append(escape(s.get("name"))).append("\">").append(escape(s.get("name"))).append("</span>");
        }
        html.append("</span></div>\n");
        for (Map<String, Object> scenario : scenarios) {
            final Map<String, Object> results = map(scenario.get("setups"));
            html.append("<details class=\"scenario\"><summary><span class=\"name\">").append(escape(scenario.get("name")))
                    .append(" <button class=\"copy\" type=\"button\" data-copy=\"").append(escape(scenario.get("name")))
                    .append("\" title=\"Copy scenario name\">copy</button>")
                    .append(" <span class=\"sub\">").append(escape(scenario.get("graph"))).append(" · ")
                    .append(escape(scenario.get("feature"))).append("</span></span><span class=\"cells\">");
            for (Map<String, Object> s : setups) {
                final Map<String, Object> result = null == results ? null : map(results.get(s.get("name")));
                if (null == result) {
                    html.append("<span></span>");
                    continue;
                }
                html.append("<span class=\"rate\" title=\"").append(escape(s.get("name"))).append("\" style=\"")
                        .append(shade(result.get("verifiedPassRate"))).append("\">").append(result.get("verified"))
                        .append("/").append(result.get("attempts")).append("</span>");
            }
            html.append("</span></summary>\n<div>\n");
            html.append("<p>").append(escape(scenario.get("intent"))).append("</p>\n");
            html.append("<div class=\"sub\">reference</div><pre>").append(escape(scenario.get("reference"))).append("</pre>\n");
            for (Map<String, Object> s : setups) {
                final Map<String, Object> result = null == results ? null : map(results.get(s.get("name")));
                if (null == result || "reference".equals(s.get("driver"))) continue;
                html.append("<div class=\"attempt\"><b>").append(escape(s.get("name"))).append("</b>");
                for (Map<String, Object> t : records(result.get("traversals"))) {
                    html.append("<div class=\"attempt\"><span class=\"tag ").append(escape(t.get("outcome"))).append("\">")
                            .append(escape(label(t.get("outcome")))).append("</span> ");
                    if (t.get("differential") != null && !"AGREES".equals(t.get("differential")))
                        html.append("<span class=\"tag ").append(escape(t.get("differential"))).append("\">")
                                .append(escape(label(t.get("differential")))).append("</span> ");
                    html.append("<span class=\"sub\">×").append(t.get("count")).append("</span>");
                    html.append(null == t.get("traversal") ? "<div class=\"sub\">no traversal</div>" :
                            "<pre>" + escape(t.get("traversal")) + "</pre>");
                    for (Map<String, Object> calls : records(t.get("tools"))) {
                        final List<Object> sequence = list(calls.get("calls"));
                        html.append("<div class=\"sub\">").append(sequence.isEmpty() ? "no tool calls" :
                                        escape(String.join(" → ", sequence.stream().map(c -> shortTool(c.toString())).
                                                toArray(String[]::new))))
                                .append(" ×").append(calls.get("count")).append("</div>");
                    }
                    html.append("</div>");
                }
                html.append("</div>\n");
            }
            html.append("</div></details>\n");
        }
    }

    private static void legend(final StringBuilder html) {
        html.append("<div class=\"legend\">");
        for (int i = 0; i < OUTCOMES.length; i++) {
            html.append("<span><i style=\"background: var(").append(OUTCOME_COLORS[i]).append(")\"></i>")
                    .append(label(OUTCOMES[i])).append("</span>");
        }
        html.append("</div>\n");
    }

    /**
     * A stacked bar of the outcome counts.
     */
    private static String outcomesSvg(final Map<String, Object> outcomes, final long attempts) {
        if (null == outcomes || attempts == 0) return "";
        final double width = 160;
        final StringBuilder svg = new StringBuilder("<svg width=\"160\" height=\"12\" role=\"img\"><title>");
        final List<String> parts = new ArrayList<>();
        for (String outcome : OUTCOMES) {
            if (outcomes.containsKey(outcome)) parts.add(label(outcome) + " " + outcomes.get(outcome));
        }
        svg.append(escape(String.join(", ", parts))).append("</title>");
        double x = 0;
        for (int i = 0; i < OUTCOMES.length; i++) {
            final Object count = outcomes.get(OUTCOMES[i]);
            if (null == count) continue;
            final double w = width * number(count).doubleValue() / attempts;
            svg.append(String.format(Locale.ROOT, "<rect x=\"%.1f\" y=\"0\" width=\"%.1f\" height=\"12\" style=\"fill: var(%s)\"/>",
                    x, w, OUTCOME_COLORS[i]));
            x += w;
        }
        return svg.append("</svg>").toString();
    }

    /**
     * The point and interval on an axis from -100 to +100 points.
     */
    private static String intervalSvg(final double point, final double lo, final double hi) {
        return String.format(Locale.ROOT, "<svg width=\"220\" height=\"24\" role=\"img\"><title>%s, 95%% interval %s to %s</title>" +
                        "<line class=\"axis\" x1=\"10\" y1=\"12\" x2=\"210\" y2=\"12\"/>" +
                        "<line class=\"zero\" x1=\"110\" y1=\"3\" x2=\"110\" y2=\"21\"/>" +
                        "<line class=\"ci\" x1=\"%.1f\" y1=\"12\" x2=\"%.1f\" y2=\"12\"/>" +
                        "<circle class=\"point\" cx=\"%.1f\" cy=\"12\" r=\"4\"/>" +
                        "<text x=\"10\" y=\"23\">-100</text><text x=\"196\" y=\"23\">+100</text></svg>",
                points(point), signed(lo), signed(hi), x(lo), x(hi), x(point));
    }

    private static double x(final double difference) {
        return 10 + (Math.max(-1, Math.min(1, difference)) + 1) * 100;
    }

    private static String rate(final Object rate) {
        return "<span class=\"rate\" style=\"" + shade(rate) + "\">" + percent(rate) + "</span>";
    }

    /**
     * A background running from red at no passes to green at all of them.
     */
    private static String shade(final Object rate) {
        if (null == rate) return "";
        return String.format(Locale.ROOT, "background: hsla(%d, 70%%, 45%%, 0.28)", Math.round(number(rate).doubleValue() * 120));
    }

    private static String percent(final Object rate) {
        return null == rate ? "–" : Math.round(number(rate).doubleValue() * 100) + "%";
    }

    private static String points(final double difference) {
        final String text = signed(difference);
        final String css = difference > 0 ? "pos" : difference < 0 ? "neg" : "";
        return "<span class=\"" + css + "\">" + text + "</span>";
    }

    private static String signed(final double difference) {
        return String.format(Locale.ROOT, "%+.1f", difference * 100);
    }

    private static String decimal(final Object value) {
        return null == value ? "unknown" : String.format(Locale.ROOT, "%.1f", number(value).doubleValue());
    }

    private static String whole(final Object value) {
        return null == value ? "unknown" : String.format(Locale.ROOT, "%,d", Math.round(number(value).doubleValue()));
    }

    private static String label(final Object outcome) {
        return String.valueOf(outcome).toLowerCase(Locale.ROOT).replace('_', ' ');
    }

    private static String join(final Object value) {
        return value instanceof Collection ? String.join(", ", ((Collection<?>) value).stream().map(String::valueOf).
                toArray(String[]::new)) : null == value ? null : value.toString();
    }

    private static String orDash(final Object value) {
        return null == value ? "–" : value.toString();
    }

    static String escape(final Object value) {
        return String.valueOf(value).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").
                replace("\"", "&quot;");
    }

    private static Number number(final Object value) {
        return null == value ? 0 : (Number) value;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> map(final Object value) {
        return (Map<String, Object>) value;
    }

    @SuppressWarnings("unchecked")
    private static List<Object> list(final Object value) {
        return null == value ? Collections.emptyList() : (List<Object>) value;
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> records(final Object value) {
        return null == value ? Collections.emptyList() : (List<Map<String, Object>>) value;
    }

    private static String load() {
        try (InputStream in = Report.class.getResourceAsStream("report.html")) {
            if (null == in) throw new IllegalStateException("Missing report.html");
            // the license header is for maintainers, not the report
            final String text = new String(in.readAllBytes(), StandardCharsets.UTF_8);
            return text.substring(text.indexOf("<!doctype html>"));
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }
}
