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
package io.github.mihkels.omb.arch.fixtures.clean;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Clean fixture obeying all {@code CommonArchitectureRules}. */
public final class CleanFixture {
    private static final Logger log = LoggerFactory.getLogger(CleanFixture.class);

    /**
     * Jackson 2 annotations stay allowed next to Jackson 3.
     *
     * @return a fixed name
     */
    @JsonProperty("name")
    public String name() {
        return "clean";
    }

    public void run() {
        log.info("clean");
    }
}
