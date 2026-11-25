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
package io.github.mihkels.omb.driver.testclean;

import io.github.mihkels.omb.driver.BenchmarkDriver;
import io.github.mihkels.omb.driver.DriverConfigs;
import io.github.mihkels.omb.driver.testclean.config.CleanDriverConfig;
import java.nio.file.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Clean driver fixture satisfying every {@code DriverArchitectureRules} check. */
public final class CleanBenchmarkDriver implements BenchmarkDriver {
    private static final Logger log = LoggerFactory.getLogger(CleanBenchmarkDriver.class);

    public CleanDriverConfig loadConfig(Path file) {
        log.debug("Loading {}", file);
        return DriverConfigs.read(file, CleanDriverConfig.class);
    }

    @Override
    public void close() {}
}
