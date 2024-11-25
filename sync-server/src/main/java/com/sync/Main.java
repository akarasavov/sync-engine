package com.sync;

import org.apache.logging.log4j.Level;
import spark.Request;
import spark.Response;
import spark.Route;
import spark.Service;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class Main {
    public static void main(String[] args) {
        Logger logger = LogManager.getLogger();
        var ipAddress = "127.0.0.1";
        var port = 8080;

        Service service = Service.ignite();
        service.ipAddress(ipAddress).port(port);

        MyRoute route = new MyRoute(logger);
        service.get("/", "*/*", route);

        service.awaitInitialization();
        logger.info("Http server started {}:{}", ipAddress, port);
    }
}

class MyRoute implements Route {
    private final Logger logger;

    MyRoute(Logger logger) {
        this.logger = logger;
    }

    @Override
    public Object handle(Request request, Response response) throws Exception {
        logger.log(Level.INFO, "Got request from: {}", request.ip());
        return "wow\n";
    }
}