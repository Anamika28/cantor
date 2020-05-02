/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.http.resources;

import com.google.gson.Gson;
import com.salesforce.cantor.Cantor;
import com.salesforce.cantor.functions.Functions;
import com.salesforce.cantor.functions.FunctionsOnCantor;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.util.*;

@Component
@Path("/functions")
@Tag(name = "Functions Resource", description = "Api for handling Cantor functions")
public class FunctionsResource {
    private static final Logger logger = LoggerFactory.getLogger(FunctionsResource.class);
    private static final String serverErrorMessage = "Internal server error occurred";

    private static final Gson parser = new Gson();

    private final Cantor cantor;
    private final Functions functions;

    @Autowired
    public FunctionsResource(final Cantor cantor) {
        this.cantor = cantor;
        this.functions = new FunctionsOnCantor(cantor);
    }

    @GET
    @Path("/")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Get list of all functions in the given namespace")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200",
                    description = "Provides the list of all functions in the namespace",
                    content = @Content(array = @ArraySchema(schema = @Schema(implementation = String.class)))),
            @ApiResponse(responseCode = "500", description = serverErrorMessage)
    })
    public Response getFunctions() throws IOException {
        logger.info("received request for all objects namespaces");
        return Response.ok(parser.toJson(this.functions.list())).build();
    }

    @GET
    @Path("/{function: .+}")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Get a function")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200",
                    description = "Provides the function with the given name",
                    content = @Content(array = @ArraySchema(schema = @Schema(implementation = String.class)))),
            @ApiResponse(responseCode = "500", description = serverErrorMessage)
    })
    public Response getFunction(@PathParam("function") final String functionName) throws IOException {
        final byte[] bytes = this.functions.get(functionName);
        if (bytes == null) {
            return Response.status(Response.Status.NOT_FOUND).build();
        }
        return Response.ok(bytes).build();
    }

    @GET
    @Path("/run/{function: .+}")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Execute 'get' method on a function")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200",
                    description = "Process and execute the function",
                    content = @Content(array = @ArraySchema(schema = @Schema(implementation = String.class)))),
            @ApiResponse(responseCode = "500", description = serverErrorMessage)
    })
    public Response getExecuteFunction(@PathParam("function") final String function,
                                       @Context final HttpServletRequest request,
                                       @Context final HttpServletResponse response) {
        logger.info("executing '{}' with get method", function);
        return doExecute(function, request, response);
    }

    @PUT
    @Path("/run/{function: .+}")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Execute put method on function query")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200",
                    description = "Process and execute the function",
                    content = @Content(array = @ArraySchema(schema = @Schema(implementation = String.class)))),
            @ApiResponse(responseCode = "500", description = serverErrorMessage)
    })
    public Response putExecuteFunction(@PathParam("function") final String function,
                                       @Context final HttpServletRequest request,
                                       @Context final HttpServletResponse response) {
        logger.info("executing '{}' with put method", function);
        return doExecute(function, request, response);
    }

    @POST
    @Path("/run/{function: .+}")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Execute post method on function query")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200",
                    description = "Process and execute the function",
                    content = @Content(array = @ArraySchema(schema = @Schema(implementation = String.class)))),
            @ApiResponse(responseCode = "500", description = serverErrorMessage)
    })
    public Response postExecuteFunction(@PathParam("function") final String function,
                                        @Context final HttpServletRequest request,
                                        @Context final HttpServletResponse response) {
        logger.info("executing '{}' with post method", function);
        return doExecute(function, request, response);
    }

    @DELETE
    @Path("/run/{function: .+}")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Execute delete method on function query")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200",
                    description = "Process and execute the function",
                    content = @Content(array = @ArraySchema(schema = @Schema(implementation = String.class)))),
            @ApiResponse(responseCode = "500", description = serverErrorMessage)
    })
    public Response deleteExecuteFunction(@PathParam("function") final String function,
                                          @Context final HttpServletRequest request,
                                          @Context final HttpServletResponse response) {
        logger.info("executing '{}' with delete method", function);
        return doExecute(function, request, response);
    }

    @PUT
    @Path("/{function: .+}")
    @Operation(summary = "Store a function")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "201", description = "Function stored"),
        @ApiResponse(responseCode = "500", description = serverErrorMessage)
    })
    public Response create(@Parameter(description = "Function identifier") @PathParam("function") final String functionName,
                           final String body) {
        try {
            this.functions.store(functionName, body);
            return Response.status(Response.Status.CREATED).build();
        } catch (IOException e) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(toString(e))
                    .build();
        }
    }

    private Response doExecute(final String function,
                               final HttpServletRequest request,
                               final HttpServletResponse response) {
        try {
            final com.salesforce.cantor.functions.Context context =
                    new com.salesforce.cantor.functions.Context(this.cantor, this.functions);
            // special parameters, http.request and http.response are passed to functions
            context.set("http.request", request);
            context.set("http.response", response);
            this.functions.run(function, context, getParams(request));

            // retrieve special parameter http.status from context
            final Object statusObject = context.get("http.status");
            final int status;
            if (statusObject instanceof String) {
                status = Integer.parseInt((String) statusObject);
            } else if (statusObject instanceof BigDecimal) {
                status = ((BigDecimal) statusObject).intValue();
            } else if (statusObject instanceof Long) {
                status = ((Long) statusObject).intValue();
            } else if (statusObject instanceof Integer) {
                status = (int) statusObject;
            } else {
                status = Response.Status.OK.getStatusCode();
            }
            final Response.ResponseBuilder builder = Response.status(status);
            // retrieve special parameter .out from context
            if (context.get("http.entity") != null) {
                builder.entity(context.get("http.entity"));
            }
            // retrieve special parameter http.headers from context
            if (context.get("http.headers") != null) {
                for (final Map.Entry<String, Object> header : ((Map<String, Object>) context.get("http.headers")).entrySet()) {
                    builder.header(header.getKey(), header.getValue());
                }
            }
            return builder.build();
        } catch (Exception e) {
            return Response.serverError()
                    .header("Content-Type", "text/plain")
                    .entity(toString(e))
                    .build();
        }
    }

    // convert http request query string parameters to a map of string to string
    private Map<String, String> getParams(final HttpServletRequest request) {
        final Map<String, String> params = new HashMap<>();
        for (final Map.Entry<String, String[]> entry : request.getParameterMap().entrySet()) {
            params.put(entry.getKey(), entry.getValue()[0]);
        }
        return params;
    }

    // convert throwable object stack trace to string
    private String toString(final Throwable throwable) {
        final StringWriter writer = new StringWriter();
        final PrintWriter printer = new PrintWriter(writer);
        throwable.printStackTrace(printer);
        return writer.toString();
    }
}
