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
package io.openmessaging.benchmark.worker.commands;

/** The phase of a workload run; a consumer pause re-enters the backlog phases mid-run. */
public enum RunPhase {
    /** Traffic before measurement, recorded but excluded from the summary. */
    WARMUP,
    /** Consumers paused while a backlog accumulates, for a backlog workload or a consumer pause. */
    BACKLOG_BUILD,
    /** Consumers resumed and draining the backlog. */
    BACKLOG_DRAIN,
    /** The measured window. */
    MEASURE
}
