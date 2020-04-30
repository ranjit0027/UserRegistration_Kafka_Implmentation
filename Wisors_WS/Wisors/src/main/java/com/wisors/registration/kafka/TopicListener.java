package com.wisors.registration.kafka;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import com.wisors.registration.domain.WsrUserAccount;
import com.wisors.registration.domain.WsrUserAddress;
import com.wisors.registration.domain.WsrUserGroupType;
import com.wisors.registration.domain.WsrUserGroupXref;
import com.wisors.registration.domain.WsrUserInGroup;
import com.wisors.registration.domain.UserInfo;

/**
 * 
 * @author Ranjit Sharma ,Wisors INC, USA
 * @since @11-04-2020
 * @version 1.0
 */

/*
@Service
public class TopicListener {

	private static final Logger log = LoggerFactory.getLogger(TopicListener.class);

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

	@KafkaListener(id="createUser", topics = "${user.registration.kafka.topic.name}", groupId = "group_id")
	// @JsonBackReference(value="create-account")
	public void listenCreateUserAccount(String userAccount) throws IOException {

		log.info("received data='{}'", userAccount);

		boolean isCreateMessage = true;

		log.info("================ LISTEN CREATE User Account =============================");
		log.info("");
		log.info("received data='{}' : " + userAccount);
		log.info("");

		UserInfo userInfo = populateWsrUserAccount(userAccount);

		try {
			log.info("INVOKING REST CLIENT");
			WsrUserAccount resUserAccount = restClient(isCreateMessage, userInfo, "", false);
			log.info("WsrUserAccount : " + resUserAccount);
		} catch (Exception e) {
			log.error(e.getMessage());
		}

	}

	@KafkaListener(id="updateUser", topics = "${user.registration.kafka.topic.name}", groupId = "group_id")
	public void listenUpdateUserAccount(String userAccount) throws IOException {

		// log.info("received data='{}'", userAccount.toString());

		log.info("================ LISTEN UPDATE User Account =============================");
		log.info("");
		log.info("received data='{}' : " + userAccount.toString());
		log.info("");

		UserInfo userInfo = populateWsrUserAccount(userAccount);
		userInfo.getWsrUserAccount().getPhone();

		try {
			log.info("INVOKING REST CLIENT FOR UPDATE");
			restClient(false, userInfo, userInfo.getWsrUserAccount().getPhone(), false);
			log.info("WsrUserAccount Updated Sucessfully :");
		} catch (Exception e) {
			log.error(e.getMessage());
		}

	}

	private UserInfo populateWsrUserAccount(String userAccount) {
		JSONObject jsonObj = new JSONObject(userAccount);
		log.info("JSON OBJ : " + jsonObj);

		log.info("");

		log.info(" wsrUserAccount : " + jsonObj.getJSONObject("wsrUserAccount"));
		WsrUserAccount wsrUserAccount = null;

		if (jsonObj.getJSONObject("wsrUserAccount") != null) {

			// Populate wsr_user_account
			wsrUserAccount = new WsrUserAccount();

			// default setting
			wsrUserAccount.setUserid(null);
			wsrUserAccount.setCreatedate(null);
			wsrUserAccount.setUpdatedate(null);

			JSONObject userAccountObj = jsonObj.getJSONObject("wsrUserAccount");
			log.info("userAccountObj ===>: " + userAccountObj);

			if (userAccountObj.get("usertype") != null) {
				wsrUserAccount.setUsertype(String.valueOf(userAccountObj.get("usertype")));
			}
			if (userAccountObj.get("firstname") != null) {
				wsrUserAccount.setFirstname(String.valueOf(userAccountObj.get("firstname")));
			}
			if (userAccountObj.get("lastname") != null) {
				wsrUserAccount.setLastname(String.valueOf(userAccountObj.get("lastname")));
			}
			if (userAccountObj.get("phone") != null) {
				wsrUserAccount.setPhone(String.valueOf(userAccountObj.get("phone")));
			}
			if (userAccountObj.get("email") != null) {
				wsrUserAccount.setEmail(String.valueOf(userAccountObj.get("email")));
			}
			if (userAccountObj.get("password") != null) {
				wsrUserAccount.setPassword(String.valueOf(userAccountObj.get("password")));
			}
			if (userAccountObj.get("activeflag") != null) {
				wsrUserAccount.setActiveflag(userAccountObj.getBoolean("activeflag"));
			}
			if (userAccountObj.get("dob") != null) {
				wsrUserAccount.setDob(String.valueOf(userAccountObj.get("dob")));
			}
			if (userAccountObj.get("gender") != null) {
				wsrUserAccount.setGender(String.valueOf(userAccountObj.get("gender")));
			}

		}

		JSONArray userAddArr = jsonObj.getJSONArray("wsrUserAddressList");

		List<WsrUserAddress> wsrUsrAddList = new ArrayList<WsrUserAddress>();
		for (int i = 0; i < userAddArr.length(); i++) {

			// *********************************************************
			// Populate wsr_user_address
			WsrUserAddress wsrUserAddress = new WsrUserAddress();

			// default setting
			wsrUserAddress.setCreatedate(null);
			wsrUserAddress.setUpdatedate(null);
			wsrUserAddress.setInactivedate(null);

			if (userAddArr.getJSONObject(i).getString("addresstype") != null) {
				wsrUserAddress.setAddresstype(userAddArr.getJSONObject(i).getString("addresstype"));
			}

			if (userAddArr.getJSONObject(i).getString("addressline1") != null) {
				wsrUserAddress.setAddressline1(userAddArr.getJSONObject(i).getString("addressline1"));
			}

			if (userAddArr.getJSONObject(i).getString("addressline2") != null) {
				wsrUserAddress.setAddressline2(userAddArr.getJSONObject(i).getString("addressline2"));
			}

			if (userAddArr.getJSONObject(i).getString("phone") != null) {
				wsrUserAddress.setPhone(userAddArr.getJSONObject(i).getString("phone"));
			}

			if (userAddArr.getJSONObject(i).getString("city") != null) {
				wsrUserAddress.setCity(userAddArr.getJSONObject(i).getString("city"));
			}

			if (userAddArr.getJSONObject(i).getString("state") != null) {
				wsrUserAddress.setState(userAddArr.getJSONObject(i).getString("state"));
			}

			if (userAddArr.getJSONObject(i).getString("country") != null) {
				wsrUserAddress.setCountry(userAddArr.getJSONObject(i).getString("country"));
			}

			if (userAddArr.getJSONObject(i).getString("postalcode") != null) {
				wsrUserAddress.setPostalcode(userAddArr.getJSONObject(i).getString("postalcode"));
			}

			if (userAddArr.getJSONObject(i).getString("email") != null) {
				wsrUserAddress.setEmail(userAddArr.getJSONObject(i).getString("email"));
			}

			if (Boolean.valueOf(userAddArr.getJSONObject(i).getBoolean("activeflag")) != null) {
				wsrUserAddress.setActiveflag(userAddArr.getJSONObject(i).getBoolean("activeflag"));
			}

			wsrUserAddress.setWsrUserAccount(wsrUserAccount);
			wsrUsrAddList.add(wsrUserAddress);
		}

		wsrUserAccount.setWsrUserAddress(wsrUsrAddList);

		JSONArray userGrpTypeArr = jsonObj.getJSONArray("wsrUserGroupTypeList");

		List<WsrUserGroupType> wsrUserGroupTypeList = new ArrayList<WsrUserGroupType>();

		List<WsrUserGroupXref> wsrUsrGrpRefList = new ArrayList<WsrUserGroupXref>();

		List<WsrUserInGroup> wsrUsrInGrpList = new ArrayList<WsrUserInGroup>();

		for (int i = 0; i < userGrpTypeArr.length(); i++) {

			WsrUserGroupType wsrUserGroupType = new WsrUserGroupType();

			String group_name = userGrpTypeArr.getJSONObject(i).getString("group_name");
			log.info("group_name : " + group_name);

			if (userGrpTypeArr.getJSONObject(i).getString("group_name") != null) {
				wsrUserGroupType.setGroup_name(userGrpTypeArr.getJSONObject(i).getString("group_name"));
			}
			wsrUserGroupType.setWsrUserAccount(wsrUserAccount);
			wsrUserGroupTypeList.add(wsrUserGroupType);

			log.info("WSR_USER_GRP_TYPE POPULATED ");

			// *********************************************************
			// populate wsr_user_group_ref
			WsrUserGroupXref wsrUserGroupRef = new WsrUserGroupXref();

			wsrUserGroupRef.setWsrUserGrpType(wsrUserGroupType);
			wsrUsrGrpRefList.add(wsrUserGroupRef);
			wsrUserGroupType.setWsrUserGroupXref(wsrUserGroupRef);

			WsrUserInGroup wsrUserInGroup = new WsrUserInGroup();

			if (userGrpTypeArr.getJSONObject(i).get("wsrUserGroupXref") != null) {

				JSONObject grpRefObj = (JSONObject) userGrpTypeArr.getJSONObject(i).get("wsrUserGroupXref");

				if (grpRefObj.get("wsrUserInGroup") != null) {

					JSONObject inGrpObj = (JSONObject) grpRefObj.get("wsrUserInGroup");

					if (inGrpObj.get("group_admin") != null) {

						log.info("****I***: " + String.valueOf(i));
						log.info("group_admin : " + inGrpObj.getBoolean("group_admin"));
						log.info("");
						wsrUserInGroup.setGroup_admin(inGrpObj.getBoolean("group_admin"));
					}
				}
			}

			wsrUserInGroup.setWsrUserAccount(wsrUserAccount);
			wsrUserInGroup.setWsrUserGrpRef(wsrUserGroupRef);

			wsrUserGroupRef.setWsrUserInGroup(wsrUserInGroup);

			wsrUsrInGrpList.add(wsrUserInGroup);

			log.info("======????=========");
		}

		wsrUserAccount.setWsrUserInGroup(wsrUsrInGrpList);

		wsrUserAccount.setWsrUserGroupType(wsrUserGroupTypeList);

		log.info("****************************************");
		log.info("");
		log.info("wsrUsrInGrpList ===>: " + wsrUsrInGrpList);
		log.info("");
		log.info("****************************************");

		UserInfo userInfo = new UserInfo();
		userInfo.setWsrUserAccount(wsrUserAccount);
		userInfo.setWsrUserAddressList(wsrUsrAddList);
		userInfo.setWsrUserGroupTypeList(wsrUserGroupTypeList);
		userInfo.setWsrUserInGroupList(wsrUsrInGrpList);

		log.info("USERINFO : " + userInfo);
		return userInfo;
	}

	@KafkaListener(id="retriveUser", topics = "${user.registration.kafka.topic.name}", groupId = "group_id")
	public void listenRetriveUserAccount(String phoneNumber) throws IOException {

		boolean isRetriveMessage = true;

		log.info("================ LISTEN GET User Account =============================");
		log.info("");
		log.info("received data='{}' : " + phoneNumber);
		log.info("");

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

		UserInfo userdata = null;
		try {

			log.info("phoneNo to be process : " + phoneNo);
			WsrUserAccount wsrUserAccount = restClient(false, userdata, phoneNo, isRetriveMessage);
			log.info("===== WsrUserAccount ===== >: " + wsrUserAccount);
			log.info("");
		} catch (Exception e) {
			log.error("Exception : " + e.getMessage());
		}
	}

	@KafkaListener(id="deleteUser" , topics = "${user.registration.kafka.topic.name}", groupId = "group_id")
	public void listenDeleteUserAccount(String phoneNumber) throws IOException {

		log.info("================ LISTEN DELETE User Account =============================");
		log.info("");
		log.info("received data='{}' : " + phoneNumber);
		log.info("");

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

		UserInfo userdata = null;
		try {
			restClient(false, userdata, phoneNo, false);

		} catch (Exception e) {
			log.error("Exception : " + e.getMessage());
		}
	}

	private WsrUserAccount restClient(boolean isCreateMessage, UserInfo userdata, String phoneNo,
			boolean isRetriveMessage) throws Exception {

		if (userdata != null && isCreateMessage == true && isRetriveMessage == false && phoneNo.isEmpty()) {

			log.info(" .... restClient POST....");

			try {

				WsrUserAccount wsrUserAccount = restTemplate.postForObject(createServiceURL, userdata,
						WsrUserAccount.class);
				return wsrUserAccount;

			} catch (Exception e) {

				log.error("IOException ==>: " + e.getMessage());

			}

		}

		if (userdata != null && isCreateMessage == false && isRetriveMessage == false && !phoneNo.isEmpty()) {

			log.info(" .... restClient PUT....");
			log.info("PHONE_NO : " + phoneNo);

			try {

				restTemplate.put(putServiceURL, userdata, phoneNo, WsrUserAccount.class);

			} catch (Exception e) {

				log.error("Exception:" + e.getMessage());

			}

		}

		if (userdata == null && isCreateMessage == false && isRetriveMessage == true && !phoneNo.isEmpty()) {

			log.info(" .... restClient GET....");

			log.info("PHONE_NO : " + phoneNo);

			WsrUserAccount wsrUserAccount = restTemplate.getForObject(retriveServiceURL, WsrUserAccount.class, phoneNo);

			return wsrUserAccount;

		}

		if (userdata == null && isCreateMessage == false && isRetriveMessage == false && !phoneNo.isEmpty()) {

			log.info(" .... restClient DELETE....");

			log.info("PHONE_NO : " + phoneNo);

			restTemplate.delete(deleteServiceURL, phoneNo);
		}

		else {
			log.error(" ");
		}

		return null;

	}

}

*/

public class TopicListener {}
