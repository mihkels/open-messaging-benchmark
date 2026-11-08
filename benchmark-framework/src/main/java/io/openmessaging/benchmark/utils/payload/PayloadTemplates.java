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
package io.openmessaging.benchmark.utils.payload;

import java.util.Arrays;
import java.util.List;
import org.jspecify.annotations.Nullable;

/**
 * Content-based {@code equals}, {@code hashCode}, and {@code toString} helpers for lists of payload
 * templates, for records whose generated methods would otherwise use array identity.
 */
public final class PayloadTemplates {

    private PayloadTemplates() {}

    /**
     * Compares two template lists by content.
     *
     * @param first the first template list, or {@code null}
     * @param second the second template list, or {@code null}
     * @return whether both lists hold the same number of templates with equal contents
     */
    public static boolean equals(@Nullable List<byte[]> first, @Nullable List<byte[]> second) {
        if (first == second) {
            return true;
        }
        if (first == null || second == null || first.size() != second.size()) {
            return false;
        }
        for (int i = 0; i < first.size(); i++) {
            if (!Arrays.equals(first.get(i), second.get(i))) {
                return false;
            }
        }
        return true;
    }

    /**
     * Hashes a template list by content.
     *
     * @param templates the template list, or {@code null}
     * @return a hash code consistent with {@link #equals(List, List)}
     */
    public static int hashCode(@Nullable List<byte[]> templates) {
        if (templates == null) {
            return 0;
        }
        int hash = 1;
        for (byte[] template : templates) {
            hash = 31 * hash + Arrays.hashCode(template);
        }
        return hash;
    }

    /**
     * Describes the templates by their sizes. Payloads can be megabytes long, so their bytes are
     * never printed.
     *
     * @param templates the template list, or {@code null}
     * @return the template count and sizes
     */
    public static String describe(@Nullable List<byte[]> templates) {
        if (templates == null) {
            return "null";
        }
        return templates.size()
                + " templates, sizes "
                + templates.stream().map(t -> t == null ? "null" : Integer.toString(t.length)).toList();
    }
}
