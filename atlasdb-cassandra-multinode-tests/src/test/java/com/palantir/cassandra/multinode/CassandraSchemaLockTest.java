/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.cassandra.multinode;

import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.hamcrest.Description;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Test;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.containers.Containers;
import com.palantir.atlasdb.containers.ThreeNodeCassandraCluster;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraKeyValueService;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraKeyValueServiceImpl;

public class CassandraSchemaLockTest {
    private static final int THREAD_COUNT = 4;

    @ClassRule
    public static final Containers CONTAINERS = new Containers(CassandraSchemaLockTest.class)
            .with(new ThreeNodeCassandraCluster());

    private final ExecutorService executorService = Executors.newFixedThreadPool(THREAD_COUNT);
    private final ListeningExecutorService listeningExecutorService = MoreExecutors.listeningDecorator(executorService);

    @After
    public void tearDown() {
        executorService.shutdown();
    }

    @Test
    public void shouldCreateTablesConsistentlyWithMultipleCassandraNodes() throws Exception {
        TableReference table1 = TableReference.createFromFullyQualifiedName("ns.table1");
        CassandraKeyValueServiceConfig config = ThreeNodeCassandraCluster.KVS_CONFIG;

        CyclicBarrier barrier = new CyclicBarrier(THREAD_COUNT);
        Callable<Void> createTable = () -> {
            CassandraKeyValueService keyValueService =
                    CassandraKeyValueServiceImpl.createForTesting(config, Optional.empty());
            barrier.await();
            keyValueService.createTable(table1, AtlasDbConstants.GENERIC_TABLE_METADATA);
            return null;
        };

        ListenableFuture<List<Void>> futureForAll = IntStream.range(0, THREAD_COUNT)
                .mapToObj(unused -> listeningExecutorService.submit(createTable))
                .collect(Collectors.collectingAndThen(Collectors.toList(), Futures::allAsList));

        futureForAll.get(4, TimeUnit.MINUTES);

        CassandraKeyValueService kvs = CassandraKeyValueServiceImpl.createForTesting(config, Optional.empty());
        assertThat(kvs.getAllTableNames(), hasItem(table1));

        assertThat(new File(CONTAINERS.getLogDirectory()),
                containsFiles(everyItem(doesNotContainTheColumnFamilyIdMismatchError())));
    }

    private static Matcher<File> containsFiles(Matcher<Iterable<File>> fileMatcher) {
        return new FeatureMatcher<File, List<File>>(
                fileMatcher,
                "Directory with files such that",
                "Directory contains") {
            @Override
            protected List<File> featureValueOf(File actual) {
                return ImmutableList.copyOf(actual.listFiles());
            }
        };
    }

    private static Matcher<File> doesNotContainTheColumnFamilyIdMismatchError() {
        return new TypeSafeDiagnosingMatcher<File>() {
            @Override
            protected boolean matchesSafely(File file, Description mismatchDescription) {
                Path path = Paths.get(file.getAbsolutePath());
                try (Stream<String> lines = Files.lines(path, StandardCharsets.ISO_8859_1)) {
                    List<String> badLines = lines.filter(line -> line.contains("Column family ID mismatch"))
                            .collect(Collectors.toList());

                    mismatchDescription
                            .appendText("file called " + file.getAbsolutePath() + " which contains lines")
                            .appendValueList("\n", "\n", "", badLines);

                    return badLines.isEmpty();
                } catch (IOException e) {
                    throw Throwables.propagate(e);
                }
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("a file with no column family ID mismatch errors");
            }
        };
    }
}
