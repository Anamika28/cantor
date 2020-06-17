/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.archive.file;

import com.salesforce.cantor.Cantor;
import com.salesforce.cantor.Events;
import com.salesforce.cantor.archive.EventsChunk;
import com.salesforce.cantor.h2.CantorOnH2;
import com.salesforce.cantor.misc.archivable.CantorArchiver;
import com.salesforce.cantor.misc.archivable.impl.ArchivableCantor;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class FileArchiveTest {
    private static final long timeframeBound = System.currentTimeMillis();
    private static final long timeframeOrigin = timeframeBound - TimeUnit.DAYS.toMillis(2);
    private static final String h2Directory = "/tmp/cantor-local-test";
    private static final String baseDirectory = "/tmp/cantor-archive-test";
    private static final long hourMillis = TimeUnit.HOURS.toMillis(1);

    private Map<String, Long> cantorH2Namespaces;
    private Cantor localCantor;
    private CantorArchiver archiver;

    @BeforeMethod
    public void setup() throws IOException {
        this.cantorH2Namespaces = new HashMap<>();
        final File baseDirectory = new File(FileArchiveTest.baseDirectory);
        baseDirectory.delete();
        baseDirectory.mkdirs();
        this.archiver = new ArchiverOnFile(FileArchiveTest.baseDirectory, hourMillis);
        this.localCantor = new ArchivableCantor(new CantorOnH2(h2Directory), archiver);
        generateData();
    }

    @AfterMethod
    public void teardown() throws IOException {
        // delete test archive
        final File baseDirectory = new File(FileArchiveTest.baseDirectory);
        final String[] entries = baseDirectory.list();
        if (entries != null) {
            for(final String file : entries) {
                final File currentFile = new File(baseDirectory.getPath(), file);
                currentFile.delete();
            }
        }
        baseDirectory.delete();
        // delete test cantor data
        for (final String cantorH2Namespace : this.cantorH2Namespaces.keySet()) {
            this.localCantor.events().drop(cantorH2Namespace);
        }
    }

    @Test
    public void testEventsArchive() throws IOException {
        for (final Map.Entry<String, Long> cantorH2Namespace : this.cantorH2Namespaces.entrySet()) {
            final List<Events.Event> totalEvents = this.localCantor.events()
                    .get(cantorH2Namespace.getKey(), timeframeOrigin, timeframeBound);

            final long endTimestamp = getFloorForWindow(cantorH2Namespace.getValue(), hourMillis) - 1;
            final List<Events.Event> events = this.localCantor.events()
                    .get(cantorH2Namespace.getKey(), timeframeOrigin, cantorH2Namespace.getValue());
            this.localCantor.events().expire(cantorH2Namespace.getKey(), cantorH2Namespace.getValue());
            validateArchive(events, cantorH2Namespace.getKey(), endTimestamp);

            // restore the events
            final List<Events.Event> restoreEvents = this.localCantor.events()
                    .get(cantorH2Namespace.getKey(), timeframeOrigin, cantorH2Namespace.getValue());
            Assert.assertEquals(restoreEvents.size(), events.size(), "all events were not restored for namespace: " + cantorH2Namespace.getKey());

            // sanity check no events have been lost
            final List<Events.Event> totalEventsAgain = this.localCantor.events()
                    .get(cantorH2Namespace.getKey(), timeframeOrigin, timeframeBound);
            Assert.assertEquals(totalEventsAgain.size(), totalEvents.size(), "more events were expired than were archived for namespace: " + cantorH2Namespace.getKey());
        }
    }

    @Test
    public void testEventsArchiveIdempotent() throws IOException {
        for (final Map.Entry<String, Long> cantorH2Namespace : this.cantorH2Namespaces.entrySet()) {
            final List<Events.Event> totalEvents = this.localCantor.events()
                    .get(cantorH2Namespace.getKey(), timeframeOrigin, timeframeBound);

            final long endTimestamp = getFloorForWindow(cantorH2Namespace.getValue(), hourMillis) - 1;
            final List<Events.Event> events = this.localCantor.events()
                    .get(cantorH2Namespace.getKey(), timeframeOrigin, cantorH2Namespace.getValue());
            this.localCantor.events().expire(cantorH2Namespace.getKey(), cantorH2Namespace.getValue());
            validateArchive(events, cantorH2Namespace.getKey(), endTimestamp);

            // run again; restoring events
            final List<Events.Event> sameEvents = this.localCantor.events()
                    .get(cantorH2Namespace.getKey(), timeframeOrigin, cantorH2Namespace.getValue());
            Assert.assertEquals(sameEvents.size(), events.size(), "all events were not restored for namespace: " + cantorH2Namespace.getKey());
            this.localCantor.events().expire(cantorH2Namespace.getKey(), cantorH2Namespace.getValue());
            // intentionally checking that noEvents had no impact by validating with events
            validateArchive(events, cantorH2Namespace.getKey(), endTimestamp);
            validateArchive(totalEvents, cantorH2Namespace.getKey(), timeframeBound);

            // run again restoring all events then expire again with one new event;
            this.localCantor.events().store(cantorH2Namespace.getKey(), timeframeOrigin, null, null);
            final List<Events.Event> allEvents = this.localCantor.events()
                    .get(cantorH2Namespace.getKey(), timeframeOrigin, timeframeBound);
            final List<Events.Event> allEventsAgain = this.localCantor.events()
                    .get(cantorH2Namespace.getKey(), timeframeOrigin, timeframeBound);
            Assert.assertEquals(allEvents.size(), allEventsAgain.size(), "incorrect number of events after second call to get events: " + cantorH2Namespace.getKey());
            this.localCantor.events().expire(cantorH2Namespace.getKey(), timeframeBound);
            validateArchive(allEventsAgain, cantorH2Namespace.getKey(), timeframeBound);

            // last run with dirtied archive file
            final List<Events.Event> refreshedEvents = this.localCantor.events()
                    .get(cantorH2Namespace.getKey(), timeframeOrigin, timeframeBound);
            Assert.assertEquals(refreshedEvents.size(), allEvents.size(), "incorrect number of events after restoration for events: " + cantorH2Namespace.getKey());

            // sanity check no events have been lost
            // plus one for the extra event we added mid test
            Assert.assertEquals(refreshedEvents.size(), totalEvents.size() + 1, "more events were expired than were archived for namespace: " + cantorH2Namespace.getKey());
        }
    }

    @Test
    public void testEventsArchiveZero() throws IOException {
        for (final String cantorH2Namespace : this.cantorH2Namespaces.keySet()) {
            final List<Events.Event> events = this.localCantor.events()
                    .get(cantorH2Namespace, 0, hourMillis);
            this.localCantor.events().expire(cantorH2Namespace, hourMillis);
            validateArchive(events, cantorH2Namespace, hourMillis);

            // restore the events
            final List<Events.Event> restoreEvents = this.localCantor.events()
                    .get(cantorH2Namespace, 0, hourMillis);
            Assert.assertEquals(restoreEvents.size(), events.size(), "all events were not restored for namespace: " + cantorH2Namespace);
        }
    }

    private void validateArchive(final List<Events.Event> events,
                                 final String cantorH2Namespace,
                                 final long endTimestamp) throws IOException {
        if (events.size() > 0) {
            int eventCount = 0;
            final EventsArchiverOnFile eventsArchiver = (EventsArchiverOnFile) this.archiver.events();
            for (long end = endTimestamp; end > 0; end -= hourMillis) {
                final Path fileArchive = eventsArchiver.getFileArchive(cantorH2Namespace, end + 1);
                if (!fileArchive.toFile().exists()) return;

                try (final ArchiveInputStream archiveInputStream = eventsArchiver.getArchiveInputStream(fileArchive)) {
                    while (archiveInputStream.getNextEntry() != null) {
                        final EventsChunk chunk = EventsChunk.parseFrom(archiveInputStream);
                        eventCount += chunk.getEventsCount();
                    }
                }
            }
            Assert.assertEquals(eventCount, events.size(), "events that were expired were not archived");
        } else {
            final String[] files = new File(baseDirectory).list();
            if (files == null) return;

            final List<String> filesList = Arrays.asList(files);
            filesList.forEach(file -> Assert.assertFalse(file.contains(cantorH2Namespace), "no events archived but found archive file for namespace: " + cantorH2Namespace));
        }
    }

    private void generateData() throws IOException {
        final ThreadLocalRandom random = ThreadLocalRandom.current();
        for (int namespaceCount = 0; namespaceCount < random.nextInt(2, 5); namespaceCount++) {
            final String namespace = "cantor-archive-test-" + Math.abs(UUID.randomUUID().hashCode());
            this.localCantor.events().create(namespace);
            this.cantorH2Namespaces.put(namespace, random.nextLong(timeframeOrigin, timeframeBound));

            for (int eventCount = 0; eventCount < random.nextInt(100, 1000); eventCount++) { // 1GB max
                final byte[] randomPayload = new byte[random.nextInt(0, 1_000_000)]; // 1MB max
                random.nextBytes(randomPayload);
                this.localCantor.events().store(
                    namespace, random.nextLong(timeframeOrigin, timeframeBound),
                        null,null, randomPayload
                );
            }
            final Map<String, String> metadataMap = new HashMap<>();
            metadataMap.put("test-event-metadata", "test-generate");
            this.localCantor.events().store(
                    namespace, timeframeOrigin - 1,
                    metadataMap,null, null
            );
            this.localCantor.events().store(
                    namespace, timeframeBound + 1,
                    metadataMap,null, null
            );
            for (int eventCount = 0; eventCount < random.nextInt(1, 10); eventCount++) { // 1MB max
                // throw in a few random events at zero timestamp
                final byte[] randomPayload = new byte[random.nextInt(0, 100_000)]; // 100KB max
                random.nextBytes(randomPayload);
                this.localCantor.events().store(
                        namespace, 0,
                        null,null, null
                );
            }
        }
    }

    private long getFloorForWindow(final long timestampMillis, final long chunkMillis) {
        return (timestampMillis / chunkMillis) * chunkMillis;
    }
}
