package br.com.kafka.main;

import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class FraudDetectorService {

	public static void main(String[] args) throws InterruptedException, ExecutionException {
		var fraudDetectorService = new FraudDetectorService();
		try (var service = new KafkaService("ECOMMERCE_NEW_ORDER", fraudDetectorService::parse,
				FraudDetectorService.class.getSimpleName())) {
			service.run();
		}
	}

	private void parse(ConsumerRecord<String, String> record) {
		System.out.println("------------------------------------------------------------------------------------");
		System.out.println("Processing new order");
		System.out.println(record.key());
		System.out.println(record.value());
		System.out.println(record.partition());
		System.out.println(record.partition());
	}
}
