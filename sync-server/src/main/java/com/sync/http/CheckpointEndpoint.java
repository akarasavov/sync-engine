package com.sync.http;

import java.util.Set;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import spark.Request;
import spark.Response;
import spark.Route;
import spark.route.HttpMethod;

public class CheckpointEndpoint implements Endpoint {
    //private final Logger logger = LogManager.getLogger(CheckpointEndpoint.class);
    private static final String CHECKPOINT_PATH = "/checkpoint";

    @Override
    public Set<EndpointRoute> getRoutes() {
        return Set.of(
                new EndpointRoute(
                        String.join("/", CHECKPOINT_PATH, "current"),
                        new CurrentRoute(),
                        HttpMethod.get
                )
        );
    }

    // TODO:
    //  * decide how to handle Route classes
    //  * also how to do logger
    //  * query params for routes
    static class CurrentRoute implements Route {
        @Override
        public Object handle(Request request, Response response) throws Exception {
            Logger logger = LogManager.getLogger(CurrentRoute.class);
            logger.log(Level.INFO, "Got request from: {}", request.ip());
            return "current\n";}
    }
}
