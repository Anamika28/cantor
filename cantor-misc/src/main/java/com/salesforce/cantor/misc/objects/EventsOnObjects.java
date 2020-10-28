/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.misc.objects;

import com.salesforce.cantor.Events;
import com.salesforce.cantor.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class EventsOnObjects implements Events {
    private static final Logger logger = LoggerFactory.getLogger(EventsOnObjects.class);
    private static final String metadataKeyPayloadGuid = ".payload-object-guid";
    private static final long minPayloadSizeBytes = 1024 * 1024;
    private final String objectsNamespace;

    private final Objects objectsDelegate;
    private final Events eventsDelegate;

    public EventsOnObjects(final Events eventsDelegate, final Objects objectsDelegates, final String objectsNamespace) {
        this.eventsDelegate = eventsDelegate;
        this.objectsDelegate = objectsDelegates;
        this.objectsNamespace = objectsNamespace;
    }

    @Override
    public void store(final String namespace, final Collection<Event> batch) throws IOException {
        final List<Event> events = new ArrayList<>(batch.size());
        final Map<String, byte[]> payloads = new HashMap<>();
        for (final Event e : batch) {
            // if there is a payload and it is larger than the min limit, then store the payload in s3
            if (e.getPayload() == null || e.getPayload().length < minPayloadSizeBytes) {
                continue;
            }
            final String payloadGuid = UUID.randomUUID().toString();
            payloads.put(payloadGuid, e.getPayload());
            e.getMetadata().put(metadataKeyPayloadGuid, payloadGuid);
            events.add(new Event(e.getTimestampMillis(), e.getMetadata(), e.getDimensions(), null));
        }
        if (!payloads.isEmpty()) {
            this.objectsDelegate.store(this.objectsNamespace, payloads);
        }
        this.eventsDelegate.store(namespace, events);
    }

    @Override
    public List<Event> get(final String namespace,
                           final long startTimestampMillis,
                           final long endTimestampMillis,
                           final Map<String, String> metadataQuery,
                           final Map<String, String> dimensionsQuery,
                           final boolean includePayloads,
                           final boolean ascending,
                           final int limit) throws IOException {
        final List<Event> results = this.eventsDelegate.get(namespace, startTimestampMillis, endTimestampMillis, metadataQuery, dimensionsQuery, includePayloads, ascending, limit);
        if (!includePayloads) {
            return results;
        }
        // if a payload guid metadata is found, fetch the payload bytes from s3 and attach to the event object
        for (final Event e : results) {
            if (!e.getMetadata().containsKey(metadataKeyPayloadGuid)) {
                continue;
            }
            final String payloadGuid = e.getMetadata().get(metadataKeyPayloadGuid);
            e.getMetadata().remove(metadataKeyPayloadGuid);
            final byte[] payload = this.objectsDelegate.get(this.objectsNamespace, payloadGuid);
            if (payload == null) {
                logger.warn("cannot find payload on S3 with guid: {}", payloadGuid);
            } else {
                results.add(new Event(e.getTimestampMillis(), e.getMetadata(), e.getDimensions(), payload));
                results.remove(e);
            }
        }
        return results;
    }

    @Override
    public int delete(final String namespace,
                      final long startTimestampMillis,
                      final long endTimestampMillis,
                      final Map<String, String> metadataQuery,
                      final Map<String, String> dimensionsQuery) throws IOException {
        return this.eventsDelegate.delete(namespace, startTimestampMillis, endTimestampMillis, metadataQuery, dimensionsQuery);
    }

    @Override
    public Map<Long, Double> aggregate(final String namespace,
                                       final String dimension,
                                       final long startTimestampMillis,
                                       final long endTimestampMillis,
                                       final Map<String, String> metadataQuery,
                                       final Map<String, String> dimensionsQuery,
                                       final int aggregateIntervalMillis,
                                       final AggregationFunction aggregationFunction) throws IOException {
        return this.eventsDelegate.aggregate(namespace, dimension, startTimestampMillis, endTimestampMillis,
                metadataQuery, dimensionsQuery, aggregateIntervalMillis, aggregationFunction
        );
    }

    @Override
    public Set<String> metadata(final String namespace,
                                final String metadataKey,
                                final long startTimestampMillis,
                                final long endTimestampMillis,
                                final Map<String, String> metadataQuery,
                                final Map<String, String> dimensionsQuery) throws IOException {
        return this.eventsDelegate.metadata(namespace, metadataKey, startTimestampMillis, endTimestampMillis, metadataQuery, dimensionsQuery);
    }

    @Override
    public void expire(final String namespace, final long endTimestampMillis) throws IOException {
        this.eventsDelegate.expire(namespace, endTimestampMillis);
    }

    @Override
    public Collection<String> namespaces() throws IOException {
        return this.eventsDelegate.namespaces();
    }

    @Override
    public void create(final String namespace) throws IOException {
        this.eventsDelegate.create(namespace);
    }

    @Override
    public void drop(String namespace) throws IOException {
        this.eventsDelegate.drop(namespace);
    }

    protected String storePayload(final byte[] payload) {

    }

    protected byte[] getPayload(final String metadataKeyPayloadGuid) {

    }
}
