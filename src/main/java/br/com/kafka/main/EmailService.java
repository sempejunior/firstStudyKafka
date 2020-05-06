package br.com.kafka.main;

import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class EmailService {

	public static void main(String[] args) throws InterruptedException, ExecutionException {
		var emailService = new EmailService();
		try (var service = new KafkaService("ECOMMERCE_SEND_EMAIL", emailService::parse,
				EmailService.class.getSimpleName())) {
			service.run();
		}
	}

	private void parse(ConsumerRecord<String, String> record) {
		System.out.println("------------------------------------------------------------------------------------");
		System.out.println("Sendind email");
		System.out.println(record.key());
		System.out.println(record.value());
		System.out.println(record.partition());
		System.out.println(record.partition());
	}

}
