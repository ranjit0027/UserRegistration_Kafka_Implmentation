package com.wisors.registration.kafka;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
//import com.wisors.registration.domain.UserAccount;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.wisors.registration.RegistrationApplication;
import com.wisors.registration.domain.UserInfo;
import com.wisors.registration.domain.WsrUserAccount;
import com.wisors.registration.domain.WsrUserAddress;
import com.wisors.registration.domain.WsrUserGroupType;
import com.wisors.registration.domain.WsrUserGroupXref;
import com.wisors.registration.domain.WsrUserInGroup;
import com.wisors.registration.exception.UserAccountNotFoundException;

import io.swagger.annotations.Api;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

/**
 * 
 * @author Ranjit Sharma ,Wisors INC, USA
 * @since @11-04-2020
 * @version 1.0
 */

@Configuration
@PropertySource(ignoreResourceNotFound = true, value = "classpath:application.properties")
@Api(value = "UserAccountController", description = "REST Apis related to Kafka")
@EnableSwagger2
@RestController
@RequestMapping("/api/kafka/users")
public class KafkaController {

	private static final Logger log = LoggerFactory.getLogger(KafkaController.class);

	@Value("${kafka.broker.name}")
	private String broker;

	@Autowired
	private TopicProducer topicProducer;

	@Value("${user.registration.kafka.topic.name}")
	private String REGISTRATION_TOPIC;

	// @Value("${user.registration.response.kafka.topic.name}")
	// private String REGISTRATION_RESPONSE_TOPIC;

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

	@Value("${retriveAllUser.response.client.id}")
	private String RETRIVE_ALL_USER_RESPONSE_USER_ID;

	@Autowired
	private KafkaTopicListener kafkaTopicListener;

	@PostMapping(consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<WsrUserAccount> createUserAccount(@RequestBody UserInfo userinfo) {

		log.info("KAFKA CREATE CONTROLLER END POINT");
		log.info("");
		log.info("USER_INFO : " + userinfo);
		log.info("");

		ResponseEntity<WsrUserAccount> registeredUser = null;

		topicProducer.sendCreateUserMessage(userinfo);

		log.info("MESSAGE SEND to Producer");

		// TODO : Invoke becouse TopicListener(Springboot KafkaLitener) is not working
		// as expected
		WsrUserAccount userAccount = kafkaTopicListener.comsumeCreateEntity(REGISTRATION_TOPIC, CREATE_USER_ID, null);
		log.info("userAccount : " + userAccount);
		JSONObject responseobj = null;
		String[] response = null;

		if (userAccount != null) {
			response = comsumeResponseEntity(REGISTRATION_TOPIC, CREATE_RESPONSE_USER_ID, null);
			responseobj = new JSONObject(response[1]);

			byte[] jsonData = responseobj.toString().getBytes();

			ObjectMapper mapper = new ObjectMapper();
			try {
				WsrUserAccount useraccount = mapper.readValue(jsonData, WsrUserAccount.class);
				registeredUser = ResponseEntity.ok().body(useraccount);
			} catch (JsonParseException e) {
				log.error(e.getMessage());
			} catch (JsonMappingException e) {
				log.error(e.getMessage());
			} catch (IOException e) {
				log.error(e.getMessage());
			}
			return registeredUser;
		} else {
			response = comsumeResponseEntity(REGISTRATION_TOPIC, null, ERROR_ID);
			responseobj = new JSONObject(response[1]);
			if (responseobj.getString("errorMessage") != null) {
				throw new UserAccountNotFoundException(responseobj.getString("errorMessage"));
			}
		}
		return registeredUser;
	}

	@PutMapping(value = "/{phoneNo}", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<WsrUserAccount> updateUserAccount(@RequestBody UserInfo userinfo,
			@PathVariable String phoneNo) {

		log.info("KAFKA UPDATE CONTROLLER END POINT");
		log.info("");
		log.info("USER_INFO : " + userinfo);
		log.info("");

		ResponseEntity<WsrUserAccount> registeredUser = null;

		topicProducer.sendUpdatUeserMessage(userinfo, phoneNo);

		log.info("MESSAGE SEND to Producer");

		// TODO : Invoke becouse TopicListener(Springboot KafkaLitener) is not working
		// as expected
		kafkaTopicListener.comsumeCreateEntity(REGISTRATION_TOPIC, UPDATE_USER_ID, phoneNo);
		JSONObject responseobj = null;
		String[] response = null;

		response = comsumeResponseEntity(REGISTRATION_TOPIC, UPDATE_RESPONSE_USER_ID, ERROR_ID);

		log.info("CLIENT_ID :" + response[0]);

		if (response[0].equals(ERROR_ID)) {
			responseobj = new JSONObject(response[1]);
			log.info("responseObj ===>: " + responseobj);

			if (responseobj.getString("errorMessage") != null) {
				throw new UserAccountNotFoundException(responseobj.getString("errorMessage"));
			}

		} else {
			responseobj = new JSONObject(response[1]);
			byte[] jsonData = responseobj.toString().getBytes();

			ObjectMapper mapper = new ObjectMapper();
			try {
				WsrUserAccount useraccount = mapper.readValue(jsonData, WsrUserAccount.class);
				log.info(" **** WsrUserAccount ***** : " + useraccount);
				registeredUser = ResponseEntity.ok().body(useraccount);
			} catch (JsonParseException e) {
				log.error(e.getMessage());
			} catch (JsonMappingException e) {
				log.error(e.getMessage());
			} catch (IOException e) {
				log.error(e.getMessage());
			}

			log.info("");
			log.info(" **** registeredUser ***** : " + registeredUser);
			log.info("");

			return registeredUser;

		}
		return registeredUser;
	}

	@GetMapping(value = "/{phoneNo}", produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<WsrUserAccount> getUserAccount(@PathVariable String phoneNo) {

		log.info("KAFKA RETRIVE CONTROLLER END POINT");
		log.info("");
		log.info("USER_INFO PHONE NO : " + phoneNo);
		log.info("");

		ResponseEntity<WsrUserAccount> registeredUser = null;

		topicProducer.sendRetriveUeserMessage(phoneNo);

		// TODO : Invoke becouse TopicListener(Springboot KafkaLitener) is not working
		// as expected
		WsrUserAccount userAccount = kafkaTopicListener.comsumeCreateEntity(REGISTRATION_TOPIC, RETRIVE_USER_ID,
				phoneNo);
		log.info("WSR_USER_ACCOUNT : " + userAccount);
		JSONObject responseobj = null;
		String[] response = null;

		if (userAccount != null) {
			log.info("USER_ACCOUNT NOT NULL");
			response = comsumeResponseEntity(REGISTRATION_TOPIC, RETRIVE_RESPONSE_USER_ID, null);

			responseobj = new JSONObject(response[1]);
			log.info("");
			log.info("^^^^^^ JSON_OBJ ^^^^^^^ " + responseobj);
			log.info("");

			byte[] jsonData = responseobj.toString().getBytes();

			ObjectMapper mapper = new ObjectMapper();
			try {
				WsrUserAccount useraccount = mapper.readValue(jsonData, WsrUserAccount.class);
				log.info(" **** WsrUserAccount ***** : " + useraccount);
				registeredUser = ResponseEntity.ok().body(useraccount);
			} catch (JsonParseException e) {
				log.error(e.getMessage());
			} catch (JsonMappingException e) {
				log.error(e.getMessage());
			} catch (IOException e) {
				log.error(e.getMessage());
			}

			log.info("");
			log.info(" **** registeredUser ***** : " + registeredUser);
			log.info("");

			return registeredUser;
		} else {
			response = comsumeResponseEntity(REGISTRATION_TOPIC, null, ERROR_ID);
			responseobj = new JSONObject(response[1]);
			if (responseobj.getString("errorMessage") != null) {
				log.info("EEE?????");
				throw new UserAccountNotFoundException(responseobj.getString("errorMessage"));
			}
		}
		return registeredUser;
	}

	@GetMapping(value = "/all", produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<String> getAllUsers() {
		log.info(" - KAFKA Get All User Accounts --.");

		topicProducer.sendRetriveAllUeserMessage();

		// TODO : Invoke becouse TopicListener(Springboot KafkaLitener) is not working
		// as expected
		WsrUserAccount userAccount = kafkaTopicListener.comsumeCreateEntity(REGISTRATION_TOPIC, RETRIVE_ALL_USER_ID,
				null);
		log.info("REST CLIENT RESPONSE userAccount ===>: " + userAccount);

		String[] response = comsumeResponseEntity(REGISTRATION_TOPIC, RETRIVE_ALL_USER_RESPONSE_USER_ID, ERROR_ID);
		log.info("-------------------------------------------------");
		log.info("response_0: " + response[0]);
		log.info("response_1 : " + response[1]);
		log.info("----------------------------------------------------");
		ResponseEntity<String> registeredUser = null;
		if (response[0].equals(RETRIVE_ALL_USER_RESPONSE_USER_ID)) {

			registeredUser = ResponseEntity.ok().body(response[1]);

			return registeredUser;
		} else {
			JSONObject responseobj = new JSONObject(response[1]);
			if (responseobj.getString("errorMessage") != null) {
				throw new UserAccountNotFoundException(responseobj.getString("errorMessage"));
			}
		}
		return registeredUser;
	}

	@DeleteMapping(value = "/{phoneNo}")
	public ResponseEntity<String> deleteUserAccount(@PathVariable String phoneNo) {

		log.info("KAFKA DELETE OPERATION ");
		topicProducer.sendDeleteUeserMessage(phoneNo);

		// TODO : Invoke becouse TopicListener(Springboot KafkaLitener) is not working
		// as expected
		kafkaTopicListener.comsumeCreateEntity(REGISTRATION_TOPIC, DELETE_USER_ID, phoneNo);

		String[] response = comsumeResponseEntity(REGISTRATION_TOPIC, DELETE_RESPONSE_USER_ID, ERROR_ID);

		log.info("DELEETD RESPOSNE : " + response[1]);

		return new ResponseEntity<String>(String.valueOf(response[1]), HttpStatus.OK);

	}

	private String[] comsumeResponseEntity(String topic, String producerId, String errorId) {

		log.info("**** comsumeResponseEntity : ****" +" topic : " + topic + " , producerID: " + producerId + " , errorId: " + errorId);

		// producerId = "createUser";

		Properties properties = new Properties();
		properties.put("bootstrap.servers", broker);

		// properties.put("bootstrap.servers", "localhost:9092");
		properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		properties.put(ConsumerConfig.GROUP_ID_CONFIG, "group_id");

		if (producerId != null) {
			properties.put(ConsumerConfig.CLIENT_ID_CONFIG, producerId);
		} else {
			properties.put(ConsumerConfig.CLIENT_ID_CONFIG, errorId);
		}

		KafkaConsumer kafkaConsumer = new KafkaConsumer(properties);

		// List topics = new ArrayList();
		// topics.add(topic);

		// kafkaConsumer.subscribe(topics);

		kafkaConsumer.subscribe(Collections.singletonList(topic));

		log.info("#############################################");
		return executeConsumer(kafkaConsumer, producerId, errorId);

	}

	private String[] executeConsumer(KafkaConsumer kafkaConsumer, String producerId, String errorId) {
		boolean flag = true;
		final int giveUp = 100;
		int noRecordsCount = 0;
		// String message = "";

		String[] message = new String[2];

		while (flag) {
			log.info("****");
			final ConsumerRecords<Long, String> consumerRecords = kafkaConsumer.poll(1000);
			log.info("????");
			if (consumerRecords.count() == 0) {
				if (noRecordsCount > giveUp) {
					noRecordsCount++;
					log.info("noRecordsCount : " + noRecordsCount);
					break;
				} else {
					log.info("NO");
					continue;
				}
			}
			// consumerRecords.forEach(record -> {
			for (ConsumerRecord record : consumerRecords) {

				log.info("==================================================================");
				// log.info("Consumer Record:(%d, %s, %d, %d)\n", record.key(), record.value(),
				// record.partition(), record.offset());

				log.info("");
				log.info("key: " + record.key() + " , " + "partition: " + record.partition() + " , " 
							+ "offset:" + record.offset() + " , value: " + record.value());
				log.info("");
				log.info("==================================================================");

				if (producerId != null && record.key().toString().equals(producerId)) {
					log.info("PRODUCT_ID");
					message[0] = record.key().toString();
					message[1] = record.value().toString();
					flag = false;
				} else if (errorId != null && record.key().toString().equals(errorId)) {
					log.info("ERROR_ID");
					message[0] = record.key().toString();
					message[1] = record.value().toString();
					flag = false;
				}
			}
			;
			kafkaConsumer.commitAsync();
		}
		kafkaConsumer.close();
		log.info("=================================================");
		return message;
	}
}
