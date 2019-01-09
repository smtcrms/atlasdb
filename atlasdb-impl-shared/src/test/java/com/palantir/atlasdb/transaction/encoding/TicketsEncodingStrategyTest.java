/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.atlasdb.transaction.encoding;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import org.junit.Test;

import com.google.protobuf.ByteString;
import com.palantir.atlasdb.keyvalue.api.Cell;

public class TicketsEncodingStrategyTest {
    private final TicketsEncodingStrategy strategy = new TicketsEncodingStrategy();

    @Test
    public void canDistinguishNumericallyCloseTimestamps() {
        assertStartTimestampsCanBeDistinguished(LongStream.range(0, 1000).toArray());
    }

    @Test
    public void canDistinguishTimestampsAcrossRows() {
        long numRows = TicketsEncodingStrategy.ROWS_PER_QUANTUM;
        long offset = 318557;
        assertStartTimestampsCanBeDistinguished(offset, numRows + offset, 2 * numRows + offset);
    }

    @Test
    public void canDistinguishTimestampsAcrossQuanta() {
        long quantum = TicketsEncodingStrategy.PARTITIONING_QUANTUM;
        long offset = 318557;
        assertStartTimestampsCanBeDistinguished(offset, quantum + offset, 2 * quantum + offset);
    }

    @Test
    public void canDistinguishTimestampsAroundPartitioningQuantum() {
        long quantum = TicketsEncodingStrategy.PARTITIONING_QUANTUM;
        assertStartTimestampsCanBeDistinguished(
                0, 1, quantum - 1, quantum, quantum + 1, 2 * quantum - 1, 2 * quantum, 2 * quantum + 1);
    }

    @Test
    public void canDistinguishTimestampsAroundRowBoundary() {
        long numRows = TicketsEncodingStrategy.ROWS_PER_QUANTUM;
        assertStartTimestampsCanBeDistinguished(0, 1, numRows - 1, numRows, numRows + 1, 2 * numRows - 1);
    }

    @Test
    public void cellEncodeAndDecodeAreInverses() {
        fuzzOneThousandTrials(() -> {
            long timestamp = ThreadLocalRandom.current().nextLong(1, Long.MAX_VALUE);
            Cell encoded = strategy.encodeStartTimestampAsCell(timestamp);
            assertThat(strategy.decodeCellAsStartTimestamp(encoded)).isEqualTo(timestamp);
        });
    }

    @Test
    public void commitTimestampEncodeAndDecodeAreInverses() {
        fuzzOneThousandTrials(() -> {
            long startTimestamp = ThreadLocalRandom.current().nextLong(0, Long.MAX_VALUE - 1);
            long commitTimestamp = ThreadLocalRandom.current().nextLong(startTimestamp, Long.MAX_VALUE);
            byte[] encoded = strategy.encodeCommitTimestampAsValue(startTimestamp, commitTimestamp);
            assertThat(strategy.decodeValueAsCommitTimestamp(startTimestamp, encoded)).isEqualTo(commitTimestamp);
        });
    }

    @Test
    public void storesLargeCommitTimestampsCompactly() {
        long highTimestamp = Long.MAX_VALUE - 1;
        byte[] commitTimestampEncoding = strategy.encodeCommitTimestampAsValue(highTimestamp, highTimestamp + 1);
        assertThat(commitTimestampEncoding.length).isEqualTo(1);
        assertThat(strategy.decodeValueAsCommitTimestamp(highTimestamp, commitTimestampEncoding))
                .isEqualTo(highTimestamp + 1);
    }

    @Test
    public void distributesTimestampsEvenlyAcrossRows() {
        int elementsExpectedPerRow = 37;
        int timestampsPerPartition = TicketsEncodingStrategy.ROWS_PER_QUANTUM * elementsExpectedPerRow;
        int numPartitions = 13;

        Set<Cell> associatedCells = IntStream.range(0, numPartitions)
                .mapToObj(partitionNumber -> IntStream.range(0, timestampsPerPartition)
                        .mapToLong(timestampIndex ->
                                TicketsEncodingStrategy.PARTITIONING_QUANTUM * partitionNumber + timestampIndex)
                        .mapToObj(strategy::encodeStartTimestampAsCell))
                .flatMap(x -> x)
                .collect(Collectors.toSet());

        // groupingBy evaluates arrays on instance equality, which is a no-go, so we need ByteStrings
        Map<ByteString, List<Cell>> cellsGroupedByRow = associatedCells.stream()
                .collect(Collectors.groupingBy(cell -> ByteString.copyFrom(cell.getRowName())));
        for (List<Cell> cellsInEachRow : cellsGroupedByRow.values()) {
            assertThat(cellsInEachRow.size()).isEqualTo(elementsExpectedPerRow);
        }
    }

    private static void fuzzOneThousandTrials(Runnable test) {
        IntStream.range(0, 1000).forEach(unused -> test.run());
    }

    private void assertStartTimestampsCanBeDistinguished(long... timestamps) {
        Set<Cell> convertedCells = Arrays.stream(timestamps)
                .boxed()
                .map(strategy::encodeStartTimestampAsCell)
                .collect(Collectors.toSet());
        assertThat(convertedCells.size()).isEqualTo(timestamps.length);
    }
}