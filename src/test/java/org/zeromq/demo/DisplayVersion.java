package org.zeromq.demo;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

public class DisplayVersion {

	private static final Logger log = LoggerFactory.getLogger(DisplayVersion.class);
	
	@Test
	public void testVersion() throws Exception {
		 log.debug(String.format("Version string: %s, Version int: %d",
	                ZMQ.getVersionString(),
	                ZMQ.getFullVersion()));
	}
}
