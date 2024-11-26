package com.sync;

import java.util.Set;
import com.sync.http.DefaultEndpoint;
import com.sync.http.Endpoint;
import com.sync.http.Server;
import com.sync.http.SparkServer;

public class Main {
    public static void main(String[] args) {
        Endpoint defaultEndpoint = new DefaultEndpoint();
        Set<Endpoint> endpoints = Set.of(defaultEndpoint);

        Server server = new SparkServer("127.0.0.1", 8080, endpoints);
        server.start();
    }
}