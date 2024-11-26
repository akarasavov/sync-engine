package com.sync.http;

import java.util.Set;
import spark.Service;


public class SparkServer implements Server {
    private final String address;
    private final int port;
    private final Set<Endpoint> endpoints;

    public SparkServer(String address, int port, Set<Endpoint> endpoints) {
        this.address = address;
        this.port = port;
        this.endpoints = endpoints;
    }

    @Override
    public void start() {
        Service service = Service.ignite();
        service.ipAddress(address).port(port);
        initRestEndpoints(service);
        service.awaitInitialization();
    }

    private void initRestEndpoints(Service service) {
        // TODO: investigate response transformers
        endpoints.forEach(e -> {
            var routes = e.getRoutes();
            routes.forEach(r -> {
                switch (r.httpMethod()) {
                    case get -> service.get(r.endpoint(), "*/*", r.route());
                }
            });
        });
    }
}
