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
		ConsumerApp cignaConsumerApp = new ConsumerApp();
		cignaConsumerApp.consume(consumerForCigna, 6);

		/*
		 * Consumer<Long, String> consumerForAetna =
		 * ConsumerFactory.createConsumer(Payer.AETNA); ConsumerApp aetnaConsumerApp =
		 * new ConsumerApp(); aetnaConsumerApp.consume(consumerForAetna, 3);
		 */

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
				System.out.println("[Consumer: " + record.topic() + " Record Key " + record.key() + " Record Message "
						+ record.value() + " : Processing Started");

				try {
					TimeUnit.SECONDS.sleep(timeTakenSeconds);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}

				System.out.println("[Consumer: " + record.topic() + " Record Key " + record.key() + " Record Message "
						+ record.value() + " : Processing Completed");
			});

			// commits the offset of record to broker.
			consumer.commitAsync();
		}
		consumer.close();
	}

}
