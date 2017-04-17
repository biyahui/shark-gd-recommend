
package com.askingdata.gd.model;

import org.apache.log4j.Logger;

import com.askingdata.shark.common.MongoSparkDriver;

public class MainProcessor {
	private static Logger logger = Logger.getLogger(MainProcessor.class);

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		MongoSparkDriver driver = new MongoSparkDriver();
		driver.setLogger(logger);
		driver.enableHiveSupport();
		driver.addExecutorClassScanPath("com.askingdata.gd.model.wish.recommend.similarity");
		driver.go(args);
		driver.close();
	}
}

