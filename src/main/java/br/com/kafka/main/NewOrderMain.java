package br.com.kafka.main;

import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

	public static void main(String[] args) throws InterruptedException, ExecutionException {

		try (var dispatcher = new KafkaDispatcher()) {
			for (int i = 0; i < 100; i++) {
				var key = UUID.randomUUID().toString();
				var value = key + ",1234561,21356523,789456";
				var email = "Thank you for you order! We are preocessing your order!";
				dispatcher.send("ECOMMERCE_NEW_ORDER", key, value);
				dispatcher.send("ECOMMERCE_SEND_EMAIL", key, email);
			}
		}
	}
}
