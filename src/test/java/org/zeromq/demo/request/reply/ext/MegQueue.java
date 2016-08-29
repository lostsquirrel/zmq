package org.zeromq.demo.request.reply.ext;

import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;

public class MegQueue {

	public static void main(String[] args) {
		// Prepare our context and sockets
		Context context = ZMQ.context(1);

		// Socket facing clients
		Socket frontend = context.socket(ZMQ.ROUTER);
		frontend.bind("tcp://*:5559");

		// Socket facing services
		Socket backend = context.socket(ZMQ.DEALER);
		backend.bind("tcp://*:5560");

		// Start the proxy
		ZMQ.proxy(frontend, backend, null);

		// We never get here but clean up anyhow
		frontend.close();
		backend.close();
		context.term();
	}
}
