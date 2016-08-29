package org.zeromq.demo;

import java.nio.charset.Charset;
import java.util.Random;
import java.util.StringTokenizer;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

public class WeatherDemo {

	private static final Logger log = LoggerFactory.getLogger(WeatherDemo.class);

	@Test
	public void testWeatherServer() throws Exception {
		// Prepare our context and publisher
		ZMQ.Context context = ZMQ.context(1);

		ZMQ.Socket publisher = context.socket(ZMQ.PUB);
		publisher.bind("tcp://*:5556");
		// publisher.bind("ipc://weather"); windows 不支持

		// Initialize random number generator
		Random srandom = new Random(System.currentTimeMillis());
		while (!Thread.currentThread().isInterrupted()) {
			// Get values that will fool the boss
			int zipcode, temperature, relhumidity;
			zipcode = 10000 + srandom.nextInt(10000);
			temperature = srandom.nextInt(215) - 80 + 1;
			relhumidity = srandom.nextInt(50) + 10 + 1;

			// Send message to all subscribers
			String update = String.format("%05d %d %d", zipcode, temperature, relhumidity);
			log.debug(String.format("publish message: %s", update));
			publisher.send(update, 0);
		}

		publisher.close();
		context.term();
	}

	@Test
	public void testWeatherClient() throws Exception {
		weatherClient("10001 ");
	}
	@Test
	public void testWeatherClient2() throws Exception {
		weatherClient("10005 ");
	}

	private void weatherClient(String filter) {
		ZMQ.Context context = ZMQ.context(1);

		// Socket to talk to server
		log.info("Collecting updates from weather server");
		ZMQ.Socket subscriber = context.socket(ZMQ.SUB);
		subscriber.connect("tcp://localhost:5556");

		// Subscribe to zipcode, default is NYC, 10001
		subscriber.subscribe(filter.getBytes());

		// Process 100 updates
		int update_nbr;
		long total_temp = 0;
		for (update_nbr = 0; update_nbr < 100; update_nbr++) {
			String charsetName = "UTF-8";
			// Use trim to remove the tailing '0' character
			String string = subscriber.recvStr(0, Charset.forName(charsetName)).trim();

			StringTokenizer sscanf = new StringTokenizer(string, " ");
			int zipcode = Integer.valueOf(sscanf.nextToken());
			int temperature = Integer.valueOf(sscanf.nextToken());
			int relhumidity = Integer.valueOf(sscanf.nextToken());

			total_temp += temperature;
			log.debug(String.format("zipcode:%s; temperature:%s; relhumidity:%s", zipcode, temperature, relhumidity));

		}
		log.debug("Average temperature for zipcode '" + filter + "' was " + (int) (total_temp / update_nbr));

		subscriber.close();
		context.term();
	}
}
