package com.sync

import io.ktor.server.response.*
import io.ktor.server.routing.*
import io.ktor.server.engine.*
import io.ktor.server.netty.*

class Application {
    fun start() {
        embeddedServer(Netty, port = 8080) {
            routing {
                get("/") {
                    call.respondText("Hello, world!")
                }
            }
        }.start(wait = true)
    }
}