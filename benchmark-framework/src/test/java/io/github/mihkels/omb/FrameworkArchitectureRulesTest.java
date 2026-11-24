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
package io.github.mihkels.omb;

import static org.assertj.core.api.Assertions.assertThat;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.lang.EvaluationResult;
import io.github.mihkels.omb.utils.payload.CompressiblePayloads;
import io.github.mihkels.omb.utils.payload.PayloadTemplates;
import org.junit.jupiter.api.Test;

/** Negative checks that {@link FrameworkArchitectureTest} rules catch real violations. */
class FrameworkArchitectureRulesTest {

    @Test
    void driverJvmRuleRejectsCompressiblePayloads() throws ClassNotFoundException {
        JavaClasses classes =
                new ClassFileImporter()
                        .importClasses(
                                Class.forName("io.github.mihkels.omb.worker.commands.CompressiblePayloadsUser"),
                                CompressiblePayloads.class,
                                PayloadTemplates.class);

        EvaluationResult result =
                FrameworkArchitectureTest.DRIVER_JVM_DOES_NOT_DEPEND_ON_SUPERVISOR_ONLY_CLASSES.evaluate(
                        classes);

        assertThat(result.hasViolation()).isTrue();
        assertThat(result.getFailureReport().getDetails())
                .anyMatch(detail ->
                        detail.contains("CompressiblePayloadsUser")
                                && detail.contains("CompressiblePayloads.ratio"));
        assertThat(result.getFailureReport().getDetails())
                .noneMatch(detail -> detail.contains("<" + PayloadTemplates.class.getName() + ">"));
    }
}
