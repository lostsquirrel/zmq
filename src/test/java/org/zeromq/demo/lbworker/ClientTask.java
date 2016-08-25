package org.zeromq.demo.lbworker;

import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;
import org.zeromq.demo.ZHelper;

public class ClientTask extends Thread {
	public void run() {
		Context context = ZMQ.context(1);

		// Prepare our context and sockets
		Socket client = context.socket(ZMQ.REQ);
		ZHelper.setId(client); // Set a printable identity

		client.connect("ipc://frontend.ipc");

		// Send request, get reply
		client.send("HELLO");
		String reply = client.recvStr();
		System.out.println("Client: " + reply);

		client.close();
		context.term();
	}
}
