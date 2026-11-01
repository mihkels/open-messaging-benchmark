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
package io.openmessaging.benchmark.utils;

import java.text.DecimalFormat;
import java.text.FieldPosition;
import java.util.Objects;

public class PaddingDecimalFormat extends DecimalFormat {
    private final int minimumLength;

    /**
     * Creates a PaddingDecimalFormat using the given pattern and minimum minimumLength and the
     * symbols for the default locale.
     *
     * @param pattern the pattern to use for formatting
     * @param minLength the minimum length of the formatted number
     */
    public PaddingDecimalFormat(String pattern, int minLength) {
        super(pattern);
        minimumLength = minLength;
    }

    @Override
    public StringBuffer format(double number, StringBuffer toAppendTo, FieldPosition pos) {
        int initLength = toAppendTo.length();
        super.format(number, toAppendTo, pos);
        return pad(toAppendTo, initLength);
    }

    @Override
    public StringBuffer format(long number, StringBuffer toAppendTo, FieldPosition pos) {
        int initLength = toAppendTo.length();
        super.format(number, toAppendTo, pos);
        return pad(toAppendTo, initLength);
    }

    private StringBuffer pad(StringBuffer toAppendTo, int initLength) {
        int numLength = toAppendTo.length() - initLength;
        int padLength = minimumLength - numLength;
        if (padLength > 0) {
            StringBuilder pad = new StringBuilder(padLength);
            pad.repeat(" ", padLength);
            toAppendTo.insert(initLength, pad);
        }
        return toAppendTo;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        PaddingDecimalFormat that = (PaddingDecimalFormat) o;
        return minimumLength == that.minimumLength;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), minimumLength);
    }
}
