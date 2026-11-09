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
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * An immutable set of payload templates and their optional weights.
 *
 * <p>The templates are copied once, when the value is created, and never again: {@link
 * #template(int)} and {@link #templates()} hand out the stored arrays, which callers must not
 * modify. {@code equals} and {@code hashCode} compare array contents, and {@code toString} prints
 * the template sizes and weights rather than the payload bytes, which can be megabytes long.
 */
public final class PayloadTemplates {

    private final List<byte[]> templates;
    private final int @Nullable [] weights;

    private PayloadTemplates(List<byte[]> templates, int @Nullable [] weights) {
        this.templates = templates;
        this.weights = weights;
    }

    /**
     * Copies the templates and their weights.
     *
     * @param templates the payload templates, or {@code null} for none
     * @param weights the weight of each template, or {@code null} to pick them uniformly
     * @return the templates
     * @throws IllegalArgumentException if the weights don't match the templates one to one
     * @throws NullPointerException if a template is {@code null}
     */
    public static PayloadTemplates of(@Nullable List<byte[]> templates, int @Nullable [] weights) {
        List<byte[]> copies =
                templates == null
                        ? List.of()
                        : templates.stream().map(t -> Objects.requireNonNull(t, "template").clone()).toList();
        if (weights != null && weights.length != copies.size()) {
            throw new IllegalArgumentException("Payload weights must match the number of payloads");
        }
        return new PayloadTemplates(copies, weights == null ? null : weights.clone());
    }

    /**
     * Returns the number of templates.
     *
     * @return the template count
     */
    public int size() {
        return templates.size();
    }

    /**
     * Returns one template without copying it.
     *
     * @param index the template index
     * @return the stored template, which must not be modified
     */
    public byte[] template(int index) {
        return templates.get(index);
    }

    /**
     * Returns an unmodifiable view of the templates without copying them.
     *
     * @return the stored templates, whose arrays must not be modified
     */
    public List<byte[]> templates() {
        return Collections.unmodifiableList(templates);
    }

    /**
     * Returns whether the templates carry weights.
     *
     * @return {@code true} if the templates are picked by weight, {@code false} if uniformly
     */
    public boolean weighted() {
        return weights != null;
    }

    /**
     * Returns a copy of the weights. The array holds one {@code int} per template, so the copy is
     * cheap, unlike copying the templates.
     *
     * @return the weight of each template, or {@code null} to pick them uniformly
     */
    public int @Nullable [] weights() {
        return weights == null ? null : weights.clone();
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof PayloadTemplates that)
                || templates.size() != that.templates.size()
                || !Arrays.equals(weights, that.weights)) {
            return false;
        }
        for (int i = 0; i < templates.size(); i++) {
            if (!Arrays.equals(templates.get(i), that.templates.get(i))) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        int hash = 1;
        for (byte[] template : templates) {
            hash = 31 * hash + Arrays.hashCode(template);
        }
        return 31 * hash + Arrays.hashCode(weights);
    }

    @Override
    public String toString() {
        return templates.size()
                + " templates, sizes "
                + templates.stream().map(t -> t.length).toList()
                + ", weights "
                + Arrays.toString(weights);
    }
}
