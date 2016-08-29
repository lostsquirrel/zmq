package org.zeromq.demo.routerdealer;

import java.nio.charset.Charset;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;
import org.zeromq.demo.ZHelper;

public class Worker extends Thread {

	private static Random rand = new Random();
	
	private static final Log log = LogFactory.getLog(Worker.class);
	
	 @Override
     public void run() {

         Context context = ZMQ.context(1);
         Socket worker = context.socket(ZMQ.DEALER);
         ZHelper.setId(worker);  //  Set a printable identity

         worker.connect("tcp://localhost:5671");

         int total = 0;
         while (true) {
             //  Tell the broker we're ready for work
             worker.sendMore("");
             worker.send("Hi Boss");

             //  Get workload from broker, until finished
             worker.recvStr(Charset.defaultCharset());   //  Envelope delimiter
             String workload = worker.recvStr(Charset.defaultCharset());
             log.debug(String.format("worker %s process workload: %s" , new String(worker.getIdentity()), workload));
             boolean finished = workload.equals("Fired!");
             if (finished) {
                 log.debug(String.format("Completed: %d tasks\n", total));
                 break;
             }
             total++;

             //  Do some random work
             try {
                 Thread.sleep(rand.nextInt(500) + 1);
             } catch (InterruptedException e) {
             }
         }
         worker.close();
         context.term();
     }
}
