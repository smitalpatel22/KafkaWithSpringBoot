package com.kafka.KafkaWithSpringBoot;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@SpringBootApplication
public class KafkaWithSpringBootApplication implements CommandLineRunner{

	public static Logger logger = LoggerFactory.getLogger(KafkaWithSpringBootApplication.class);
	public static void main(String[] args) {
		SpringApplication.run(KafkaWithSpringBootApplication.class, args);
	}


	@Autowired
	private KafkaTemplate<String, String> template;

	private final CountDownLatch latch = new CountDownLatch(3);


	@Override
	public void run(String... args) throws Exception {
		template.send("helloworld.t","pqr");
		template.send("helloworld.t","xyz");
		template.send("helloworld.t","def");
		latch.await(5, TimeUnit.SECONDS);
		logger.info("All msgs received");
	}


	@KafkaListener(topics="helloworld.t")
	public void listen(ConsumerRecord<?,?> cr)
	{
		logger.info(cr.value().toString());
		System.out.println("abc");
		latch.countDown();
	}
}
