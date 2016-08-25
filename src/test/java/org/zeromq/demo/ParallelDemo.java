package org.zeromq.demo;

import static org.junit.Assert.*;

import java.util.Random;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

public class ParallelDemo {

	private static final Logger log = LoggerFactory.getLogger(ParallelDemo.class);

	@Test
	public void testTaskVent() throws Exception {
		ZMQ.Context context = ZMQ.context(1);

		// Socket to send messages on
		ZMQ.Socket sender = context.socket(ZMQ.PUSH);
		sender.bind("tcp://*:5557");

		// Socket to send messages on
		ZMQ.Socket sink = context.socket(ZMQ.PUSH);
		sink.connect("tcp://localhost:5558");

		log.info("Press Enter when the workers are ready: ");
		System.in.read();
		log.info("Sending tasks to workers\n");

		// The first message is "0" and signals start of batch
		sink.send("0", 0);

		// Initialize random number generator
		Random srandom = new Random(System.currentTimeMillis());

		// Send 100 tasks
		int task_nbr;
		int total_msec = 0; // Total expected cost in msecs
		for (task_nbr = 0; task_nbr < 100; task_nbr++) {
			int workload;
			// Random workload from 1 to 100msecs
			workload = srandom.nextInt(100) + 1;
			total_msec += workload;
			log.debug(workload + ".");
			String string = String.format("%d", workload);
			sender.send(string, 0);
		}
		log.info("Total expected cost: " + total_msec + " msec");
		Thread.sleep(1000); // Give 0MQ time to deliver

		sink.close();
		sender.close();
		context.term();
	}

	@Test
	public void testTaskWorker() throws Exception {
		
		String worker = "w1";
		taskWorker(worker);
	}
	@Test
	public void testTaskWorker2() throws Exception {
		
		String worker = "w2";
		taskWorker(worker);
	}
	@Test
	public void testTaskWorker3() throws Exception {
		
		String worker = "w3";
		taskWorker(worker);
	}

	private void taskWorker(String worker) throws InterruptedException {
		ZMQ.Context context = ZMQ.context(1);

		// Socket to receive messages on
		ZMQ.Socket receiver = context.socket(ZMQ.PULL);
		receiver.connect("tcp://localhost:5557");

		// Socket to send messages to
		ZMQ.Socket sender = context.socket(ZMQ.PUSH);
		sender.connect("tcp://localhost:5558");
		log.info(String.format("worker %s ready ...", worker));
		// Process tasks forever
		long processTotal = 0;
		while (!Thread.currentThread().isInterrupted()) {
			String string = new String(receiver.recv(0)).trim();
			long msec = Long.parseLong(string);
			// Simple progress indicator for the viewer
			// System.out.flush();
			processTotal += msec;
			log.info(String.format("worker %s receive message %s . total: %d", worker, string, processTotal));

			// Do the work
			Thread.sleep(msec);

			// Send results to sink
			sender.send(String.format("worker %s result for message %s .", worker, string).getBytes(), 0);
		}
		sender.close();
		receiver.close();
		context.term();
	}

	@Test
	public void testTaskWorker2c() throws Exception {
		ZMQ.Context context = ZMQ.context(1);

		ZMQ.Socket receiver = context.socket(ZMQ.PULL);
		receiver.connect("tcp://localhost:5557");

		ZMQ.Socket sender = context.socket(ZMQ.PUSH);
		sender.connect("tcp://localhost:5558");

		ZMQ.Socket controller = context.socket(ZMQ.SUB);
		controller.connect("tcp://localhost:5559");
		controller.subscribe("".getBytes());

		ZMQ.Poller items = new ZMQ.Poller(2);
		items.register(receiver, ZMQ.Poller.POLLIN);
		items.register(controller, ZMQ.Poller.POLLIN);

		while (true) {

			items.poll();

			if (items.pollin(0)) {

				String message = new String(receiver.recv(0)).trim();
				long nsec = Long.parseLong(message);

				// Simple progress indicator for the viewer
				log.info(message + '.');

				// Do the work
				Thread.sleep(nsec);

				// Send results to sink
				sender.send("", 0);
			}
			// Any waiting controller command acts as 'KILL'
			if (items.pollin(1)) {
				break; // Exit loop
			}

		}

		// Finished
		receiver.close();
		sender.close();
		controller.close();
		context.term();
	}
	
	@Test
	public void testTaskSink() throws Exception {
		 //  Prepare our context and socket
        ZMQ.Context context = ZMQ.context(1);
        ZMQ.Socket receiver = context.socket(ZMQ.PULL);
        receiver.bind("tcp://*:5558");
        log.debug("sink ready ...");
        //  Wait for start of batch
        String string = new String(receiver.recv(0));
        log.debug("sink receive message: " + string);
        //  Start our clock now
        long tstart = System.currentTimeMillis();

        //  Process 100 confirmations
        int task_nbr;
//        int total_msec = 0;     //  Total calculated cost in msecs
        for (task_nbr = 0; task_nbr < 100; task_nbr++) {
            string = new String(receiver.recv(0)).trim();
            if ((task_nbr / 10) * 10 == task_nbr) { // 整10
                log.info(":");
            } else {
                log.info(".");
            }
        }
        //  Calculate and report duration of batch
        long tend = System.currentTimeMillis();

        log.info("\nTotal elapsed time: " + (tend - tstart) + " msec");
        receiver.close();
        context.term();
    
	}
}
