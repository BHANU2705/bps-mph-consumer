package com.mph.core.consumer;

import java.time.Duration;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import com.mph.core.commons.IKafkaConstants;

public class ConsumerApp {

	public static void main(String[] args) {
		Consumer<Long, String> consumer = ConsumerFactory.createConsumer();

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
				System.out.println("[Consumer] Record Key " + record.key()
				+ " Record value " + record.value()
				+ " Record partition " + record.partition()
				+ " Record offset " + record.offset());
			});

			// commits the offset of record to broker.
			consumer.commitAsync();
		}
		consumer.close();

	}

}
