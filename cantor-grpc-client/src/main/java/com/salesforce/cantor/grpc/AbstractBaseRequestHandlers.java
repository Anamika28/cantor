/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.grpc;

import com.google.protobuf.Message;
import com.salesforce.cantor.Objects;
import com.salesforce.cantor.common.CommonPreconditions;
import com.salesforce.cantor.grpc.objects.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static com.salesforce.cantor.common.CommonPreconditions.checkArgument;

public abstract class AbstractBaseRequestHandlers {
    private final Map<Class<? extends Message>, RequestHandler> handlers = new HashMap<>();

    public AbstractBaseRequestHandlers(final Objects delegate) {
        this.handlers.put(NamespacesRequest.class, new NamespaceRequestHandler(delegate));
        this.handlers.put(CreateRequest.class, new CreateRequestHandler(delegate));
    }

    public Message handle(final Message request) throws IOException {
        return this.handlers.get(request.getClass()).handle(request);
    }

    protected void registerHandler(final Class<? extends Message> requestClassType, final RequestHandler handler) {
        checkArgument(!this.handlers.containsKey(requestClassType),
                "handler for class type already exists: " + requestClassType.getName());
        this.handlers.put(requestClassType, handler);
    }

    protected static abstract class RequestHandler {
        private final Objects delegate;

        private RequestHandler(final Objects delegate) {
            this.delegate = delegate;
        }

        protected Objects getDelegate() {
            return this.delegate;
        }

        abstract Message handle(Message message) throws IOException;
    }

    private static class NamespaceRequestHandler extends RequestHandler {
        private NamespaceRequestHandler(final Objects delegate) {
            super(delegate);
        }

        @Override
        public NamespacesResponse handle(final Message ignored) throws IOException {
            final NamespacesResponse response = NamespacesResponse.newBuilder()
                    .addAllNamespaces(getDelegate().namespaces())
                    .build();
            return response;
        }
    }

    private static class CreateRequestHandler extends RequestHandler {
        private CreateRequestHandler(final Objects delegate) {
            super(delegate);
        }

        @Override
        public VoidResponse handle(final Message message) throws IOException {
            final CreateRequest request = (CreateRequest) message;
            getDelegate().create(request.getNamespace());
            return VoidResponse.getDefaultInstance();
        }
    }

    private static class DropRequestHandler extends RequestHandler {
        private DropRequestHandler(final Objects delegate) {
            super(delegate);
        }

        @Override
        public VoidResponse handle(final Message message) throws IOException {
            final DropRequest request = (DropRequest) message;
            getDelegate().create(request.getNamespace());
            return VoidResponse.getDefaultInstance();
        }
    }
}
