package org.zeromq.demo.request.reply.ext;

import java.nio.charset.Charset;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;

public class Client {
	
	private static final Log log = LogFactory.getLog(Client.class);
	
	public static void main(String[] args) {
		Context context = ZMQ.context(1);

		// Socket to talk to server
		Socket requester = context.socket(ZMQ.REQ);
		// connect to frontend
		requester.connect("tcp://localhost:5559");

		log.info("launch and connect client.");

		// send 
		for (int request_nbr = 0; request_nbr < 10; request_nbr++) {
			requester.send(String.format("%s:Hello", Thread.currentThread().getName()), 0);
			String reply = requester.recvStr(0, Charset.defaultCharset());
			log.debug("Received reply " + request_nbr + " [" + reply + "]");
		}

		requester.close();
		context.term();
		log.info("message send finished");
	}
}
