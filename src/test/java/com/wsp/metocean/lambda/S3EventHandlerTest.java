package com.wsp.metocean.lambda;

import com.amazonaws.services.lambda.runtime.events.S3Event;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class S3EventHandlerTest {
    @Test
    void returnsOkOnEmptyEvent() {
        S3EventHandler handler = new S3EventHandler();
        String result = handler.handleRequest(new S3Event(null), null);
        assertEquals("OK", result);
    }
}
