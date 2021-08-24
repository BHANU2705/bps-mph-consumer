package com.mph.core.consumer;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import com.mph.core.commons.IKafkaConstants;
import com.mph.core.commons.Payer;

public class ConsumerApp {

	public static void main(String[] args) {
		Consumer<Long, String> consumerForCigna = ConsumerFactory.createConsumer(Payer.CIGNA);
		Consumer<Long, String> consumerForAetna = ConsumerFactory.createConsumer(Payer.AETNA);
		ConsumerApp app = new ConsumerApp();
		app.consume(consumerForAetna, 3);
		app.consume(consumerForCigna, 6);
	}

	private void consume(Consumer<Long, String> consumer, int timeTakenSeconds) {
		int noMessageFound = 0;

		while (true) {
			ConsumerRecords<Long, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));
			// 1000 is the time in milliseconds consumer will wait if no record is found at
			// broker.
			if (consumerRecords.count() == 0) {
				noMessageFound++;
				if (noMessageFound > IKafkaConstants.MAX_NO_MESSAGE_FOUND_COUNT)
					// If no message found count is reached to threshold exit loop.
					break;
				else
					continue;
			}

			// print each record.
			consumerRecords.forEach(record -> {
				try {
					System.out.println("[Consumer: "+ record.topic() + "Record Key " + record.key()
					+ " : Processing Started");
					TimeUnit.SECONDS.sleep(timeTakenSeconds);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				System.out.println("[Consumer: "+ record.topic() + "Record Key " + record.key()
				+ " : Processing Completed");
			});

			// commits the offset of record to broker.
			consumer.commitAsync();
		}
		consumer.close();
	}

}
