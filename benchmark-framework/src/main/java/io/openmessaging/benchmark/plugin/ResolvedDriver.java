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
package io.openmessaging.benchmark.plugin;

import java.util.List;

/**
 * The bundles needed to run a driver class: the bundle exporting it first, followed by bundles
 * exporting driver classes referenced from its configuration.
 *
 * @param driverClass the driver class to instantiate
 * @param bundles the bundles to load, primary bundle first
 */
public record ResolvedDriver(String driverClass, List<PluginBundle> bundles) {

    public ResolvedDriver {
        bundles = List.copyOf(bundles);
    }

    public PluginBundle primary() {
        return bundles.get(0);
    }
}
