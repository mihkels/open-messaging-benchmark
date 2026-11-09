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

import java.util.ArrayList;
import java.util.List;
import org.jspecify.annotations.Nullable;

public final class ListPartition {

    private ListPartition() {}

    /**
     * partition a list to specified size.
     *
     * @param originList the list to partition
     * @param size the number of partitions
     * @param <T> the element type
     * @return the partitioned list
     */
    public static <T> List<List<T>> partitionList(@Nullable List<T> originList, int size) {

        List<List<T>> resultList = new ArrayList<>();
        if (null == originList || originList.isEmpty() || size <= 0) {
            return resultList;
        }
        for (int i = 0; i < size; i++) {
            resultList.add(new ArrayList<>());
        }
        int count = 0;
        for (T item : originList) {
            int index = count % size;
            resultList.get(index).add(item);
            count++;
        }
        return resultList;
    }
}
