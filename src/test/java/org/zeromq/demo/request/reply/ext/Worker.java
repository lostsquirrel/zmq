package org.zeromq.demo.request.reply.ext;

import java.nio.charset.Charset;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;

public class Worker {

	private static final Log log = LogFactory.getLog(Broker.class);
	
	public static void main(String[] args) throws Exception {
		Context context = ZMQ.context(1);

		// Socket to talk to server
		Socket responder = context.socket(ZMQ.REP);
		// connect to backend
		responder.connect("tcp://localhost:5560");

		while (!Thread.currentThread().isInterrupted()) {
			// Wait for next request from client
			String string = responder.recvStr(0, Charset.defaultCharset());
			log.debug(String.format("Received request: [%s]\n", string));

			// Do some 'work'
			Thread.sleep(1000);

			// Send reply back to client
			responder.send(String.format("%s: world", Thread.currentThread().getName()));
		}

		// We never get here but clean up anyhow
		responder.close();
		context.term();
	}
}
