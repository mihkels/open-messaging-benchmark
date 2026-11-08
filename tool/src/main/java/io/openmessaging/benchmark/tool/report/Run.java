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

import io.openmessaging.benchmark.TestResult;
import io.openmessaging.benchmark.worker.jackson.ObjectMappers;
import java.nio.file.Path;
import tools.jackson.databind.ObjectReader;

/**
 * One benchmark result file.
 *
 * @param name the file name, used to label the run in reports
 * @param result the parsed result
 */
record Run(String name, TestResult result) {
    // Unknown properties are ignored, so files written by older and newer versions both load
    private static final ObjectReader READER =
            ObjectMappers.DEFAULT.mapper().readerFor(TestResult.class);

    /**
     * Reads a result file.
     *
     * @throws tools.jackson.core.JacksonException if the file cannot be read or parsed
     */
    static Run read(Path file) {
        TestResult result = READER.readValue(file.toFile());
        return new Run(String.valueOf(file.getFileName()), result);
    }

    /**
     * Whether the file has the run metadata and summary of schema version 2. {@code schemaVersion}
     * cannot tell, because a version 1 file without the field reads back as the default 2.
     */
    boolean hasMetadata() {
        return result.metadata != null;
    }
}
