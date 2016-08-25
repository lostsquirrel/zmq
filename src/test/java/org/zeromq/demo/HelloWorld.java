package org.zeromq.demo;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

public class HelloWorld {

	private static final Logger log = LoggerFactory.getLogger(HelloWorld.class);
	@Test
	public void testServer() throws Exception {
		ZMQ.Context context = ZMQ.context(1);

        //  Socket to talk to clients
        ZMQ.Socket responder = context.socket(ZMQ.REP);
        responder.bind("tcp://*:5555");

        while (!Thread.currentThread().isInterrupted()) {
            // Wait for next request from the client
            byte[] request = responder.recv(0);
            log.debug(String.format("Received Message: %s", new String(request)));
            // Do some 'work'
            Thread.sleep(1000);

            // Send reply back to client
            String reply = "World";
            responder.send(reply.getBytes(), 0);
        }
        responder.close();
        context.term();
	}
	
	@Test
	public void testClient() throws Exception {
		ZMQ.Context context = ZMQ.context(1);

        //  Socket to talk to server
        log.debug("Connecting to hello world server…");

        ZMQ.Socket requester = context.socket(ZMQ.REQ);
        requester.connect("tcp://localhost:5555");

        for (int requestNbr = 0; requestNbr != 10; requestNbr++) {
            String request = "Hello";
            log.debug("Sending Hello " + requestNbr);
            requester.send(request.getBytes(), 0);

            byte[] reply = requester.recv(0);
            log.debug("Received " + new String(reply) + " " + requestNbr);
        }
        requester.close();
        context.term();
	}
}
