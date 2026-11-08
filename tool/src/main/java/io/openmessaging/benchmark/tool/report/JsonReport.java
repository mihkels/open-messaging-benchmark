/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.openmessaging.benchmark.tool.report;

import io.openmessaging.benchmark.tool.report.Comparison.Row;
import io.openmessaging.benchmark.tool.report.RunContext.SettingDifference;
import java.util.Collection;
import java.util.function.Function;
import org.jspecify.annotations.Nullable;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ArrayNode;
import tools.jackson.databind.node.ObjectNode;

/**
 * The JSON form of a {@link DiffReport}. Values that are not available, such as the relative delta
 * from a zero baseline, are null.
 */
final class JsonReport {
    /** Version of this output's shape; bumped on incompatible changes. */
    static final int SCHEMA_VERSION = 1;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private JsonReport() {}

    static String render(DiffReport report) {
        ObjectNode root = MAPPER.createObjectNode();
        root.put("schemaVersion", SCHEMA_VERSION);
        root.put("workload", report.workload());
        root.put("latencySource", report.latencySource().name().toLowerCase(java.util.Locale.ROOT));
        strings(root.putArray("baselineFiles"), report.baselineFiles());
        strings(root.putArray("candidateFiles"), report.candidateFiles());
        strings(root.putArray("warnings"), report.warnings());

        ArrayNode settings = root.putArray("settingDifferences");
        for (SettingDifference difference : report.settingDifferences()) {
            ObjectNode setting = settings.addObject();
            setting.put("name", difference.name());
            strings(setting.putArray("baseline"), difference.baseline());
            strings(setting.putArray("candidate"), difference.candidate());
        }

        ArrayNode rows = root.putArray("rows");
        for (Row row : report.rows()) {
            ObjectNode node = rows.addObject();
            node.put("metric", row.metric().id);
            node.put("unit", row.metric().unit.symbol);
            node.put(
                    "direction",
                    row.metric().direction == Metric.Direction.HIGHER_IS_BETTER ? "higher" : "lower");
            spread(node, "baseline", row.baseline());
            spread(node, "candidate", row.candidate());
            number(node, "delta", row.delta());
            number(node, "deltaPercent", row.deltaPercent());
            node.put("status", row.status().name().toLowerCase(java.util.Locale.ROOT));
        }

        bencher(root.putObject("baseline"), report, Row::baseline);
        bencher(root.putObject("candidate"), report, Row::candidate);
        return MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(root);
    }

    // Bencher Metric Format: {"<benchmark>": {"<measure>": {"value", "lower_value", "upper_value"}}}
    private static void bencher(
            ObjectNode side, DiffReport report, Function<Row, @Nullable Spread> spreadOfSide) {
        ObjectNode measures = side.putObject(report.workload());
        for (Row row : report.rows()) {
            Spread spread = spreadOfSide.apply(row);
            if (spread != null) {
                ObjectNode measure = measures.putObject(row.metric().id);
                measure.put("value", spread.median());
                measure.put("lower_value", spread.min());
                measure.put("upper_value", spread.max());
            }
        }
    }

    private static void spread(ObjectNode node, String field, @Nullable Spread spread) {
        if (spread == null) {
            node.putNull(field);
            return;
        }
        ObjectNode value = node.putObject(field);
        value.put("median", spread.median());
        value.put("min", spread.min());
        value.put("max", spread.max());
        value.put("runs", spread.runs());
    }

    private static void number(ObjectNode node, String field, double value) {
        if (Double.isNaN(value)) {
            node.putNull(field);
        } else {
            node.put(field, value);
        }
    }

    private static void strings(ArrayNode array, Collection<String> values) {
        values.forEach(array::add);
    }
}
