package com.sync.http;

import java.util.Set;
import spark.Route;
import spark.route.HttpMethod;

public interface Endpoint {
    Set<EndpointRoute> getRoutes();

    record EndpointRoute(String endpoint, Route route, HttpMethod httpMethod) {}
}
