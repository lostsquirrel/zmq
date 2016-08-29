package org.zeromq.demo.lbworker;

import java.nio.charset.Charset;

import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;
import org.zeromq.demo.ZHelper;

public class WorkerTask extends Thread {
	public void run() {
		Context context = ZMQ.context(1);
		// Prepare our context and sockets
		Socket worker = context.socket(ZMQ.REQ);
		ZHelper.setId(worker); // Set a printable identity

		worker.connect("ipc://backend.ipc");

		// Tell backend we're ready for work
		worker.send("READY");

		while (!Thread.currentThread().isInterrupted()) {
			String address = worker.recvStr(Charset.defaultCharset());
			String empty = worker.recvStr(Charset.defaultCharset());
			assert (empty.length() == 0);

			// Get request, send reply
			String request = worker.recvStr(Charset.defaultCharset());
			System.out.println("Worker: " + request);

			worker.sendMore(address);
			worker.sendMore("");
			worker.send("OK");
		}
		worker.close();
		context.term();
	}
}
