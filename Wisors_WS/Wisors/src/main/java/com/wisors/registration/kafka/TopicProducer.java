package com.wisors.registration.kafka;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Primary;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.wisors.registration.domain.UserInfo;
import com.wisors.registration.domain.WsrUserAccount;
import com.wisors.registration.exception.RegistrationError;
import com.wisors.registration.exception.UserAccountNotFoundException;

/**
 * 
 * @author Ranjit Sharma ,Wisors INC, USA
 * @since @11-04-2020
 * @version 1.0
 */

@Service
public class TopicProducer {

	private static final Logger log = LoggerFactory.getLogger(TopicProducer.class);

	@Value("${user.registration.kafka.topic.name}")
	private String REGISTRATION_TOPIC;

	@Value("${user.registration.response.kafka.topic.name}")
	private String REGISTRATION_RESPONSE_TOPIC;

	@Autowired
	private KafkaTemplate<String, UserInfo> createUserKafkaTemplate;

	@Autowired
	private KafkaTemplate<String, UserInfo> updateUserKafkaTemplate;

	@Autowired
	@Qualifier("retriveUserKafkaTemplate")
	private KafkaTemplate<String, String> retriveUserKafkaTemplate;

	@Autowired
	@Qualifier("deleteUserKafkaTemplate")
	private KafkaTemplate<String, String> deleteUserKafkaTemplate;

	@Autowired
	private KafkaTemplate<String, Integer> retriveAllUserKafkaTemplate;

	@Autowired
	private KafkaTemplate<String, RegistrationError> errorKafkaTemplate;

	@Autowired
	@Qualifier("errorKafkaTemplate2")
	private KafkaTemplate<String, String> errorKafkaTemplate2;

	@Autowired
	private KafkaTemplate<String, WsrUserAccount> createresponseKafkaTemplate;

	@Autowired
	private KafkaTemplate<String, WsrUserAccount> updateresponseKafkaTemplate;

	@Autowired
	private KafkaTemplate<String, WsrUserAccount> retriveresponseKafkaTemplate;

	@Autowired
	@Qualifier("deleteresponseKafkaTemplate")
	private KafkaTemplate<String, String> deleteresponseKafkaTemplate;

	
	@Autowired(required = true)
	private KafkaTemplate<String, List<WsrUserAccount>> retriveAllResponseKafkaTemplate; 

	public void sendCreateUserMessage(UserInfo userInfo) {
		log.info("Recived create message : " + userInfo.toString());
		this.createUserKafkaTemplate.send(REGISTRATION_TOPIC, "createUser", userInfo);
	}

	public void sendUpdatUeserMessage(UserInfo userinfo, String phoneNo) {
		log.info("Recived update message: " + userinfo.toString() + " , " + phoneNo);
		this.updateUserKafkaTemplate.send(REGISTRATION_TOPIC, "updateUser", userinfo);
	}

	public void sendRetriveUeserMessage(String phoneno) {
		log.info("Recived search message : " + phoneno);
		this.retriveUserKafkaTemplate.send(REGISTRATION_TOPIC, "retriveUser", String.valueOf(phoneno));
	}

	public void sendDeleteUeserMessage(String phoneno) {

		log.info("Received delete message : " + phoneno);
		this.deleteUserKafkaTemplate.send(REGISTRATION_TOPIC, "deleteUser", String.valueOf(phoneno));
	}

	public void sendRetriveAllUeserMessage() {
		log.info("Received Retrive ALL  message :");
		this.retriveAllUserKafkaTemplate.send(REGISTRATION_TOPIC, "retriveAllUser", 0);
	}

	public void sendCreateUserAccountResponseMessage(WsrUserAccount wsrUserAct, RegistrationError error,
			String errorMsg) {
		log.info("Recived responce create message  ==> : " + wsrUserAct);
		log.info("Recived responce create error  ==> : " + error);
		log.info("Recived responce create errorMsg  ==> : " + errorMsg);

		if (wsrUserAct != null && error == null && errorMsg == null) {
			this.createresponseKafkaTemplate.send(REGISTRATION_RESPONSE_TOPIC, "createUser", wsrUserAct);
		} else if (wsrUserAct == null && error != null && errorMsg == null) {
			this.errorKafkaTemplate.send(REGISTRATION_RESPONSE_TOPIC, "error", error);
		}
	}

	public void sendUpdateUserAccountResponseMessage(WsrUserAccount wsrUserAct, RegistrationError error,
			String errorMsg) {
		log.info("Recived responce update message ==> : " + wsrUserAct);

		if (wsrUserAct != null && error == null && errorMsg == null) {
			this.updateresponseKafkaTemplate.send(REGISTRATION_RESPONSE_TOPIC, "updateUser", wsrUserAct);
		} else if (wsrUserAct == null && error != null && errorMsg == null) {
			this.errorKafkaTemplate.send(REGISTRATION_RESPONSE_TOPIC, "error", error);
		}
	}

	public void sendRetriveUserAccountResponseMessage(WsrUserAccount wsrUserAct, RegistrationError error,
			String errorMsg) {
		log.info("Recived responce retrive message ==> : " + wsrUserAct);
		log.info("Recived response retrive errorMsg ==> : " + errorMsg);
		log.info("Recived responce retrive error ==> : " + error);

		if (wsrUserAct != null && error == null && errorMsg == null) {
			this.retriveresponseKafkaTemplate.send(REGISTRATION_RESPONSE_TOPIC, "retriveUser", wsrUserAct);
		} else if (wsrUserAct == null && error != null && errorMsg == null) {
			this.errorKafkaTemplate.send(REGISTRATION_RESPONSE_TOPIC, "error", error);
		} else {
			this.errorKafkaTemplate2.send(REGISTRATION_RESPONSE_TOPIC, "error2", errorMsg);
		}

	}

	public void sendDeleteUserAccountResponseMessage(String str, HttpStatus ok) {
		log.info("Recived response delete message ==> : " + HttpStatus.OK);
		this.errorKafkaTemplate2.send(REGISTRATION_RESPONSE_TOPIC, "deleteUser", str);

	}

	public void sendRetriveAllUserAccountResponseMessage(List<WsrUserAccount> list) {
		log.info("Recived response retrive all  message ==> : " + list);
		this.retriveAllResponseKafkaTemplate.send(REGISTRATION_RESPONSE_TOPIC, "retriveAllUser", list);

	}

}
