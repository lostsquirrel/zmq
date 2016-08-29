package org.zeromq.demo.routerdealer;

import java.nio.charset.Charset;

import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;

public class RouterDealerDemo {

	
    private static final int NBR_WORKERS = 10;
    
    /**
     * While this example runs in a single process, that is just to make
     * it easier to start and stop the example. Each thread has its own
     * context and conceptually acts as a separate process.
     */
    public static void main (String[] args) throws Exception {
        Context context = ZMQ.context(1);
        Socket broker = context.socket(ZMQ.ROUTER);
        broker.bind("tcp://*:5671");

        
        for (int workerNbr = 0; workerNbr < NBR_WORKERS; workerNbr++)
        {
            Thread worker = new Worker();
            worker.start();
        }

        //  Run for five seconds and then tell workers to end
        long endTime = System.currentTimeMillis() + 5000;
        
        int workersFired = 0;
        
        while (true) {
            //  Next message gives us least recently used worker
            String identity = broker.recvStr(Charset.defaultCharset());
            broker.sendMore(identity);
            broker.recv(0);     //  Envelope delimiter
            broker.recv(0);     //  Response from worker
            broker.sendMore("");

            //  Encourage workers until it's time to fire them
            if (System.currentTimeMillis() < endTime)
                broker.send("Work harder");
            else {
            // 发送结束哨兵
                broker.send("Fired!");
                if (++workersFired == NBR_WORKERS)
                    break;
            }
        }

        broker.close();
        context.term();
    }
}
