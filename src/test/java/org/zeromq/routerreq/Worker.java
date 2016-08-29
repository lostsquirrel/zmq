package org.zeromq.routerreq;

import java.nio.charset.Charset;
import java.util.Random;

import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;
import org.zeromq.demo.ZHelper;

public class Worker extends Thread {
	private static Random rand = new Random();
	
	@Override
	public void run() {

		Context context = ZMQ.context(1);
		Socket worker = context.socket(ZMQ.REQ);
		ZHelper.setId(worker); // Set a printable identity

		worker.connect("tcp://localhost:5671");

		int total = 0;
		while (true) {
			// Tell the broker we're ready for work
			worker.send("Hi Boss");

			// Get workload from broker, until finished
			String workload = worker.recvStr(Charset.defaultCharset());
			boolean finished = workload.equals("Fired!");
			if (finished) {
				System.out.printf("Completed: %d tasks\n", total);
				break;
			}
			total++;

			// Do some random work
			try {
				Thread.sleep(rand.nextInt(500) + 1);
			} catch (InterruptedException e) {
			}
		}
		worker.close();
		context.term();
	}
}
