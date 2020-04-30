package com.wisors.registration.kafka;

import java.io.IOException;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.wisors.registration.domain.UserInfo;
import com.wisors.registration.domain.WsrUserAccount;



@Service
public class KafkaTopicListener {

	private static final Logger log = LoggerFactory.getLogger(KafkaTopicListener.class);
	
	@Value("${kafka.broker.name}")
	private String broker;

	@Autowired
	private TopicProducer topicProducer;

	@Value("${user.registration.response.kafka.topic.name}")
	private String REGISTRATION_RESPONSE_TOPIC;
	
	
	@Value("${user.registration.kafka.topic.name}")
	private String REGISTRATION_TOPIC;
	
	
	@Autowired
	RestTemplate restTemplate;

	ResponseEntity<WsrUserAccount> response;

	@Value("${createServiceURL}")
	private String createServiceURL;

	@Value("${putServiceURL}")
	private String putServiceURL;

	@Value("${retriveServiceURL}")
	private String retriveServiceURL;

	@Value("${deleteServiceURL}")
	private String deleteServiceURL;
	
	@Value("${retriveAllServiceURL}")
	private String retriveAllServiceURL;
	

	public WsrUserAccount comsumeCreateEntity(String topic, String producerId, String receivedPhoneNo) {

		log.info(" .... comsumeCreateEntity ...." + topic + " , " + producerId + " , " + receivedPhoneNo);
		UserInfo userInfo = null;
		WsrUserAccount resUserAccount = null;

		if (producerId.equals("retriveUser") || producerId.equals("deleteUser")) {

			String phoneNumber = comsumeResponseEntity(topic, producerId);
			char firstChar = phoneNumber.charAt(0);
			char lastChar = phoneNumber.charAt(phoneNumber.length() - 1);
			log.info("firstChar : " + firstChar);
			log.info("lastChar : " + lastChar);
			log.info("");

			char specialChr = '"';
			String phoneNo = "";

			if (firstChar == specialChr && lastChar == specialChr) {
				log.info("MATCHINNG SPECIAL CHAR");
				for (int i = 0; i < phoneNumber.length() - 2; i++) {
					phoneNo = phoneNo + phoneNumber.charAt(i + 1);
				}
				log.info("phoneNo with specialchar:" + phoneNo);

			} else {
				phoneNo = phoneNumber;
				log.info("phoneNo without specialchar: " + phoneNo);
			}

			if (producerId.equals("retriveUser")) {

				try {
					log.info("INVOKING REST ENDPOINT VIA REST CLIENT RETRIVE  ");
					resUserAccount = restClient(false, userInfo, phoneNo, true);
					log.info("resUserAccount : " + resUserAccount);
					return resUserAccount;
				} catch (Exception e) {
					log.error(e.getMessage());
				}

				log.info(" Retrive WsrUserAccount : " + resUserAccount);
			}

			else if (producerId.equals("deleteUser")) {

				try {
					log.info("INVOKING REST ENDPOINT VIA REST CLIENT  DELETE");

					restClient(false, userInfo, phoneNo, false);
				} catch (Exception e) {
					log.error(e.getMessage());
				}

				log.info("Deleted User Data : ");
			}

		}
		else if (producerId.equals("retriveAllUser")) {
			log.info("INVOKING REST CLIENT RETRIVE ALL ");
			try {
				resUserAccount = restClient(false, userInfo, receivedPhoneNo, false);
				log.info("resUserAccount : " + resUserAccount);
			} catch (Exception e) {
				log.error(e.getMessage());
			}
			log.info("Updated WsrUserAccount : " + resUserAccount);	
		}
	
		else {

			String response = comsumeResponseEntity(topic, producerId);
			log.info("===========================");
			log.info("response ::::: " + response);
			log.info("===========================");
			
			
			JSONObject responseobj = new JSONObject(response);

			byte[] jsonData = responseobj.toString().getBytes();

			ObjectMapper mapper = new ObjectMapper();
			try {
				userInfo = mapper.readValue(jsonData, UserInfo.class);
				log.info(" **** WsrUserAccount ***** : " + userInfo);
			} catch (JsonParseException e) {
				log.error(e.getMessage());
			} catch (JsonMappingException e) {
				log.error(e.getMessage());
			} catch (IOException e) {
				log.error(e.getMessage());
			}
			
			try {
				
				if (producerId.equals("createUser")) {
					log.info("INVOKING REST CLIENT CREATE ");
					resUserAccount = restClient(true, userInfo, receivedPhoneNo, false);
					log.info("Created WsrUserAccount ==> : " + resUserAccount);
					return resUserAccount;
				} else if (producerId.equals("updateUser")) {
					log.info("INVOKING REST CLIENT UPDATE ");
					resUserAccount = restClient(false, userInfo, receivedPhoneNo, false);
					log.info("Updated WsrUserAccount : " + resUserAccount);
				}
				
			} catch (Exception e) {
				log.error(e.getMessage());
			}
		}
		return resUserAccount;
	}

	
	private String comsumeResponseEntity(String topic, String producerId) {

		log.info("**** comsumeResponseEntity : ****" + topic);

		// producerId = "createUser";

		Properties properties = new Properties();
		properties.put("bootstrap.servers", broker);

		// properties.put("bootstrap.servers", "localhost:9092");
		properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		properties.put(ConsumerConfig.GROUP_ID_CONFIG, "group_id");
		properties.put(ConsumerConfig.CLIENT_ID_CONFIG, producerId);

		KafkaConsumer kafkaConsumer = new KafkaConsumer(properties);

		// List topics = new ArrayList();
		// topics.add(topic);

		// kafkaConsumer.subscribe(topics);

		kafkaConsumer.subscribe(Collections.singletonList(topic));

		log.info("#############################################");
		return executeConsumer(kafkaConsumer);

	}

	private String executeConsumer(KafkaConsumer kafkaConsumer) {
		boolean flag = true;
		final int giveUp = 100;
		int noRecordsCount = 0;
		String message = "";

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
				log.info("key: " + record.key() + " , " + "value: " + record.value() + " , " + " , " + "partition: "
						+ record.partition() + " , " + "offset:" + record.offset());
				log.info("");
				log.info("==================================================================");
				message = record.value().toString();
				flag = false;

			}
			;
			kafkaConsumer.commitAsync();
		}
		kafkaConsumer.close();

		log.info("");
		log.info("======== > CONSUME KAFKA MESSAGE =======> : " + message);
		log.info("");
		log.info(" RETRIVE JSON DATA FROM KAFKA CONSUME MESSAGE ");
		log.info("=================================================");

		return message;

	}
	
	private WsrUserAccount restClient(boolean isCreateMessage, UserInfo userdata, String phoneNo,
			boolean isRetriveMessage) throws Exception {
		
		log.info("----------------------------------------------------");
		
		log.info("isCreateMessage : " + isCreateMessage +  " , phoneno: " + phoneNo + " , "
				+ "isRetriveMessage: " + isRetriveMessage + " , userData : " + userdata);
		
		log.info("----------------------------------------------------");
		
		WsrUserAccount wsrUserAccount = null;

		if (isCreateMessage == true && userdata != null &&  phoneNo ==null && isRetriveMessage == false) {
			log.info(" .... restClient POST....");
			try {
				wsrUserAccount = restTemplate.postForObject(createServiceURL, userdata, WsrUserAccount.class);
				return wsrUserAccount;

			} catch (Exception e) {
				log.error("IOException ==>: " + e.getMessage());
			}
		}

		if (isCreateMessage == false && userdata != null &&  phoneNo !=null && isRetriveMessage == false) {
			log.info(" .... restClient PUT....");
			log.info("PHONE_NO : " + phoneNo);
			try {
				restTemplate.put(putServiceURL, userdata, phoneNo, WsrUserAccount.class);
			} catch (Exception e) {

				log.error("Exception:" + e.getMessage());
			}
		}

		if (isCreateMessage == false && userdata == null && phoneNo !=null  && isRetriveMessage == true ) {
			log.info(" .... restClient GET....");
			log.info("PHONE_NO : " + phoneNo);
			try {
				wsrUserAccount = restTemplate.getForObject(retriveServiceURL, WsrUserAccount.class, phoneNo);				
			} catch (Exception e) {
				log.error("Exception:" + e.getMessage());
			}
			return wsrUserAccount;
		}

		if (isCreateMessage == false && userdata == null && phoneNo !=null && isRetriveMessage == false) {
			log.info(" .... restClient DELETE....");
			log.info("PHONE_NO : " + phoneNo);
			try {
				restTemplate.delete(deleteServiceURL, phoneNo);
				
			} catch (Exception e) {
				log.error("Exception:" + e.getMessage());
			}
		}
		if (isCreateMessage == false && userdata == null && phoneNo ==null && isRetriveMessage == false) {
			log.info(" .... restClient RETRIVE ALL....");
			try {
				wsrUserAccount = restTemplate.getForObject(retriveAllServiceURL, WsrUserAccount.class);				
				
			} catch (Exception e) {
				log.error("Exception:" + e.getMessage());
			}
		}
		else {
			log.error(" ERROR : ");
		}
		
		return wsrUserAccount;
	}

}




