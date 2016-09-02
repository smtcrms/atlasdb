/**
 * Copyright 2016 Palantir Technologies
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.keyvalue.dbkvs.util;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class DbKvsPartitioners {
    private DbKvsPartitioners() {
        // Utility class
    }

    /**
     * Partitions the provided map into batches, where the total count of every batch except the last is {@code limit}.
     * Note this means that a single element may be split into multiple batches. The ordering of the provided map is
     * preserved, i.e. if x appears before y in {@code counts}, then no batch containing x will appear after a batch
     * containing y, and if a batch contains both, then x will appear before y in that batch.
     */
    public static <T> List<Map<T, Integer>> partitionByTotalCount(Map<T, Integer> counts, int limit) {
        List<Map<T, Integer>> batches = new ArrayList<>();
        Map<T, Integer> currentBatch = new LinkedHashMap<>();
        batches.add(currentBatch);
        int currBatchColumns = 0;

        T currElem = null;
        int remainingCountForCurrElem = 0;
        Iterator<T> iter = counts.keySet().iterator();
        while (remainingCountForCurrElem > 0 || iter.hasNext()) {
            if (remainingCountForCurrElem == 0) {
                currElem = iter.next();
                remainingCountForCurrElem = counts.getOrDefault(currElem, 0);
            }

            if (currBatchColumns + remainingCountForCurrElem > limit) {
                // Fill up current batch
                int columnsToInclude = limit - currBatchColumns;
                if (columnsToInclude > 0) {
                    currentBatch.put(currElem, columnsToInclude);
                }
                remainingCountForCurrElem -= columnsToInclude;

                // Create new batch. Note that since we have exceeded the limit, the next iteration will ensure that
                // this new batch is non-empty.
                currentBatch = new LinkedHashMap<>();
                batches.add(currentBatch);
                currBatchColumns = 0;
            } else {
                currentBatch.put(currElem, remainingCountForCurrElem);
                currBatchColumns += remainingCountForCurrElem;
                remainingCountForCurrElem = 0;
            }
        }
        return batches;
    }
}
