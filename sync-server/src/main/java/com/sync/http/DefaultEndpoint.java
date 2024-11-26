package com.sync.http;

import java.util.Set;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import spark.Request;
import spark.Response;
import spark.Route;
import spark.route.HttpMethod;
import org.apache.logging.log4j.LogManager;

public class DefaultEndpoint implements Endpoint {
    private final Logger logger = LogManager.getLogger(DefaultEndpoint.class);

    public DefaultEndpoint() {}

    @Override
    public Set<EndpointRoute> getRoutes() {
        return Set.of(new EndpointRoute("/", new DefaultRoute(), HttpMethod.get));
    }

    class DefaultRoute implements Route {
        @Override
        public Object handle(Request request, Response response) throws Exception {
            logger.log(Level.INFO, "Got request from: {}", request.ip());
            return "wow\n";}
    }
}
