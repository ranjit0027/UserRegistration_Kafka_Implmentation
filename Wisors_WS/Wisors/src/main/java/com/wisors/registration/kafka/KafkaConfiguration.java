package com.wisors.registration.kafka;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import com.wisors.registration.RegistrationApplication;
import com.wisors.registration.domain.UserInfo;
//import com.wisors.registration.util.Utility;
import com.wisors.registration.domain.WsrUserAccount;
import com.wisors.registration.exception.RegistrationError;
import com.wisors.registration.exception.UserAccountNotFoundException;

import org.springframework.kafka.core.KafkaTemplate;

/**
 * 
 * @author Ranjit Sharma ,Wisors INC, USA
 * @since @11-04-2020
 * @version 1.0
 */

@Configuration
public class KafkaConfiguration {

	private static final Logger log = LoggerFactory.getLogger(KafkaConfiguration.class);

	@Value("${kafka.broker.name}")
	private String broker;

	@Value("${createuser.client.id}")
	private String CREATE_USER_ID;

	@Value("${updateuser.client.id}")
	private String UPDATE_USER_ID;

	@Value("${retriveuser.client.id}")
	private String RETRIVE_USER_ID;

	@Value("${deleteuser.client.id}")
	private String DELETE_USER_ID;

	@Value("${error.client.id}")
	private String ERROR_ID;

	@Value("${error2.client.id}")
	private String ERROR2_ID;

	@Value("${retriveAllUser.client.id}")
	private String RETRIVE_ALL_USER_ID;

	@Value("${createuser.response.client.id}")
	private String CREATE_RESPONSE_USER_ID;

	@Value("${updateuser.response.client.id}")
	private String UPDATE_RESPONSE_USER_ID;

	@Value("${retriveuser.response.client.id}")
	private String RETRIVE_RESPONSE_USER_ID;

	@Value("${deleteuser.response.client.id}")
	private String DELETE_RESPONSE_USER_ID;

	@Bean
	public KafkaTemplate<String, UserInfo> createUserKafkaTemplate() {
		log.info("createUserKafkaTemplate");
		Map<String, Object> config = createConfiguration(CREATE_USER_ID);
		return new KafkaTemplate<String, UserInfo>(new DefaultKafkaProducerFactory(config));
	}

	@Bean
	public KafkaTemplate<String, UserInfo> updateUserKafkaTemplate() {
		log.info("updateUserKafkaTemplate");
		Map<String, Object> config = createConfiguration(UPDATE_USER_ID);
		return new KafkaTemplate<String, UserInfo>(new DefaultKafkaProducerFactory(config));
	}

	@Bean("retriveUserKafkaTemplate")
	public KafkaTemplate<String, String> retriveUserKafkaTemplate() {
		log.info("retriveUserKafkaTemplate");
		Map<String, Object> config = createConfiguration(RETRIVE_USER_ID);
		return new KafkaTemplate<String, String>(new DefaultKafkaProducerFactory(config));
	}

	// @Primary
	@Bean("deleteUserKafkaTemplate")
	public KafkaTemplate<String, String> deleteUserKafkaTemplate() {
		log.info("deleteUserKafkaTemplate");
		Map<String, Object> config = createConfiguration(DELETE_USER_ID);
		return new KafkaTemplate<String, String>(new DefaultKafkaProducerFactory(config));
	}

	@Bean
	public KafkaTemplate<String, Integer> retriveAllUserKafkaTemplate() {
		log.info("retriveAllUserKafkaTemplate");
		Map<String, Object> config = createConfiguration(RETRIVE_ALL_USER_ID);
		return new KafkaTemplate<String, Integer>(new DefaultKafkaProducerFactory(config));
	}

	@Bean
	public KafkaTemplate<String, RegistrationError> errorKafkaTemplate() {
		log.info("errorKafkaTemplate");
		Map<String, Object> config = createConfiguration(ERROR_ID);
		return new KafkaTemplate<String, RegistrationError>(new DefaultKafkaProducerFactory(config));
	}

	@Bean("errorKafkaTemplate2")
	public KafkaTemplate<String, String> errorKafkaTemplate2() {
		log.info("error2KafkaTemplate");
		Map<String, Object> config = createConfiguration(ERROR2_ID);
		return new KafkaTemplate<String, String>(new DefaultKafkaProducerFactory(config));
	}

	@Bean
	public KafkaTemplate<String, WsrUserAccount> createresponseKafkaTemplate() {
		Map<String, Object> config = createConfiguration(CREATE_RESPONSE_USER_ID);
		return new KafkaTemplate<String, WsrUserAccount>(new DefaultKafkaProducerFactory(config));
	}

	@Bean
	public KafkaTemplate<String, WsrUserAccount> updateresponseKafkaTemplate() {
		Map<String, Object> config = createConfiguration(UPDATE_RESPONSE_USER_ID);
		return new KafkaTemplate<String, WsrUserAccount>(new DefaultKafkaProducerFactory(config));
	}

	@Bean
	public KafkaTemplate<String, WsrUserAccount> retriveresponseKafkaTemplate() {
		Map<String, Object> config = createConfiguration(RETRIVE_RESPONSE_USER_ID);
		return new KafkaTemplate<String, WsrUserAccount>(new DefaultKafkaProducerFactory(config));
	}

	@Bean("deleteresponseKafkaTemplate")
	public KafkaTemplate<String, String> deleteresponseKafkaTemplate() {
		Map<String, Object> config = createConfiguration(DELETE_RESPONSE_USER_ID);
		return new KafkaTemplate<String, String>(new DefaultKafkaProducerFactory(config));
	}

	@Bean
	public KafkaTemplate<String, List<WsrUserAccount>> retriveAllResponseKafkaTemplate() {
		Map<String, Object> config = createConfiguration(RETRIVE_ALL_USER_ID);
		return new KafkaTemplate<String, List<WsrUserAccount>>(new DefaultKafkaProducerFactory(config));
	}

	private Map<String, Object> createConfiguration(String producerId) {
		Map<String, Object> config = new HashMap<>();

		int retries = 2;

		config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);

		config.put(ProducerConfig.ACKS_CONFIG, "all");

		config.put(ProducerConfig.CLIENT_ID_CONFIG, producerId);

		config.put(ProducerConfig.RETRIES_CONFIG, retries);

		// Linger up to 100 ms before sending batch if size not met
		config.put(ProducerConfig.LINGER_MS_CONFIG, 100);

		// Buffer.memory controls the total amount of memory available to the producer
		// for buffering
		config.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);

		config.put(ProducerConfig.BATCH_SIZE_CONFIG, 16_384 * 4);

		// Batch up to 64K buffer sizes.
		config.put(ProducerConfig.BATCH_SIZE_CONFIG, 16_384 * 4);

		// Use Snappy compression for batch compression.
		config.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

		config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

		log.info(" === config ===> : " + config);

		return config;
	}

}
