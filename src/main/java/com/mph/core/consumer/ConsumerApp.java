package com.mph.core.consumer;

import java.time.Duration;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import com.mph.core.commons.IKafkaConstants;
import com.mph.core.commons.Payer;

public class ConsumerApp {

	public static void main(String[] args) {
		Consumer<Long, String> consumerForCigna = ConsumerFactory.createConsumer(Payer.CIGNA);
		Consumer<Long, String> consumerForAetna = ConsumerFactory.createConsumer(Payer.AETNA);

		int noMessageFound = 0;

		while (true) {
			ConsumerRecords<Long, String> cignaConsumerRecords = consumerForCigna.poll(Duration.ofMillis(1000));
			ConsumerRecords<Long, String> aetnaConsumerRecords = consumerForAetna.poll(Duration.ofMillis(1000));
			// 1000 is the time in milliseconds consumer will wait if no record is found at
			// broker.
			if (cignaConsumerRecords.count() == 0) {
				noMessageFound++;
				if (noMessageFound > IKafkaConstants.MAX_NO_MESSAGE_FOUND_COUNT)
					// If no message found count is reached to threshold exit loop.
					break;
				else
					continue;
			}

			// print each record.
			cignaConsumerRecords.forEach(record -> {
				System.out.println("[Consumer] Record Key " + record.key()
				+ " Record value " + record.value()
				+ " Record partition " + record.partition()
				+ " Record offset " + record.offset());
			});

			// commits the offset of record to broker.
			consumerForCigna.commitAsync();
		}
		consumerForCigna.close();

	}

}
