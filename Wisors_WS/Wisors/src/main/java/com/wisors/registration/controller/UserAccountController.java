package com.wisors.registration.controller;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import javax.persistence.Cacheable;
import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Convert;
import javax.persistence.FetchType;
import javax.persistence.ManyToOne;

import org.apache.catalina.User;
import org.json.HTTP;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.CachePut;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.wisors.registration.exception.RegistrationError;
import com.wisors.registration.exception.UserAccountNotFoundException;
//import com.wisors.registration.kafka.TopicListener;
import com.wisors.registration.kafka.TopicProducer;
import com.wisors.registration.domain.WsrUserAccount;
import com.wisors.registration.domain.WsrUserAddress;
import com.wisors.registration.domain.WsrUserGroupType;
import com.wisors.registration.domain.WsrUserGroupXref;
import com.wisors.registration.domain.WsrUserInGroup;
import com.wisors.registration.service.UserAccountService;
import com.wisors.registration.domain.UserInfo;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

//import org.springframework.cache.annotation.Cacheable;

/**
 * 
 * @author Ranjit Sharma Wisors INC, USA
 * @since @11-04-2020
 * @version 1.0
 */

@Configuration
@PropertySource(ignoreResourceNotFound = true, value = "classpath:application.properties")
@Api(value = "UserAccountController", description = "REST Apis related to User Account Registration!!!!")
@EnableSwagger2
@RestController
@RequestMapping("/api/registration/users")
public class UserAccountController {

	private static final Logger log = LoggerFactory.getLogger(UserAccountController.class);

	@Autowired(required = true)
	UserAccountService wsrUserAccountService;

	@Autowired(required = true)
	WsrUserAddress wsrUserAddressService;

	@Autowired(required = true)
	WsrUserInGroup WsrUserInGroup;

	@Autowired
	private TopicProducer topicProducer;

	@Autowired
	public UserAccountController(UserAccountService wsrUserAccountService) {
		this.wsrUserAccountService = wsrUserAccountService;

	}

	// @CrossOrigin(origins = "http://localhost:3000")
	@PostMapping(consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<WsrUserAccount> createUserAccount(@RequestBody UserInfo userinfo)
			throws UserAccountNotFoundException {

		log.info("");
		log.info("================ WSC  create User Account =======================");
		log.info(" ===== userinfo ====>: " + userinfo);
		log.info("");

		boolean userAvailable = false;
		int alreadyAvialblePhoneNo = 0;

		ResponseEntity<WsrUserAccount> registeredUser = null;
		int noOfUsersAvil = wsrUserAccountService.getUsers().size();
		if (noOfUsersAvil != 0) {

			List<WsrUserAccount> userAccounts = wsrUserAccountService.getUsers();
			for (WsrUserAccount wsrUserAccount : userAccounts) {
				alreadyAvialblePhoneNo++;
				if (wsrUserAccount.getPhone().equals(userinfo.getWsrUserAccount().getPhone())) {
					log.info("PHONE NO is Already Avaiable ");
					userAvailable = true;

					topicProducer.sendCreateUserAccountResponseMessage(null, 
							new RegistrationError(404 , "The User with Phone No: ".
							concat(userinfo.getWsrUserAccount().getPhone()).concat(" is already registred")), null);

					throw new UserAccountNotFoundException("The User with Phone No : "
							.concat(userinfo.getWsrUserAccount().getPhone()).concat(" is already registred"));
				} else {
					userAvailable = false;
					if (alreadyAvialblePhoneNo == noOfUsersAvil) {
						topicProducer.sendCreateUserAccountResponseMessage(null, null,"User IS NOT EXISTS");
						log.error("PHONE NO IS NOT EXISTS IN DB ");						
					}

				}

			}
		}

		if (wsrUserAccountService.getUsers().size() >= 0 && !userAvailable) {

			log.info("*****************");

			userinfo.getWsrUserAccount().setActiveflag(true);

			// Populate wsr_user_account DB : 1st Table
			WsrUserAccount wsrUserAccount = new WsrUserAccount();

			wsrUserAccount.setUserid(userinfo.getWsrUserAccount().getUserid());
			wsrUserAccount.setUsertype(userinfo.getWsrUserAccount().getUsertype());
			wsrUserAccount.setFirstname(userinfo.getWsrUserAccount().getFirstname());
			wsrUserAccount.setLastname(userinfo.getWsrUserAccount().getLastname());
			wsrUserAccount.setPhone(userinfo.getWsrUserAccount().getPhone());
			wsrUserAccount.setEmail(userinfo.getWsrUserAccount().getEmail());
			wsrUserAccount.setPassword(userinfo.getWsrUserAccount().getPassword());

			wsrUserAccount.setActiveflag(userinfo.getWsrUserAccount().isActiveflag());

			DateFormat dateformat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			// dateformat.setTimeZone(TimeZone.getTimeZone("UTC"));

			Date createdate = new Date();

			userinfo.getWsrUserAccount().setCreatedate(dateformat.format(createdate));

			wsrUserAccount.setCreatedate(userinfo.getWsrUserAccount().getCreatedate());

			// String updatedate = Clock.systemUTC().instant().toString();
			userinfo.getWsrUserAccount().setUpdatedate(dateformat.format(createdate));
			wsrUserAccount.setUpdatedate(userinfo.getWsrUserAccount().getUpdatedate());

			Calendar calendar = Calendar.getInstance();
			calendar.setTime(createdate);

			calendar.add(Calendar.YEAR, 1);
			calendar.add(Calendar.MONTH, 0);
			calendar.add(Calendar.DATE, -1);
			calendar.add(Calendar.HOUR, 1);
			calendar.add(Calendar.MINUTE, 1);
			calendar.add(Calendar.SECOND, 1);

			Date inactivedate = calendar.getTime();
			userinfo.getWsrUserAccount().setInactivedate(dateformat.format(inactivedate));
			wsrUserAccount.setInactivedate(userinfo.getWsrUserAccount().getInactivedate());

			wsrUserAccount.setDob(userinfo.getWsrUserAccount().getDob());
			wsrUserAccount.setGender(userinfo.getWsrUserAccount().getGender());

			log.info(" userinfo.getWsrUserAddressList() : " + userinfo.getWsrUserAddressList());

			List<WsrUserAddress> wsrUsrAddList = new ArrayList<WsrUserAddress>();
			for (int i = 0; i < userinfo.getWsrUserAddressList().size(); i++) {

				// *********************************************************
				// Populate wsr_user_address DB : 2nd table
				WsrUserAddress wsrUserAddress = new WsrUserAddress();

				wsrUserAddress.setAddresstype(userinfo.getWsrUserAddressList().get(i).getAddresstype());
				wsrUserAddress.setAddressline1(userinfo.getWsrUserAddressList().get(i).getAddressline1());
				wsrUserAddress.setAddressline2(userinfo.getWsrUserAddressList().get(i).getAddressline2());
				wsrUserAddress.setPhone(userinfo.getWsrUserAddressList().get(i).getPhone());
				wsrUserAddress.setCity(userinfo.getWsrUserAddressList().get(i).getCity());
				wsrUserAddress.setState(userinfo.getWsrUserAddressList().get(i).getState());
				wsrUserAddress.setCountry(userinfo.getWsrUserAddressList().get(i).getCountry());
				wsrUserAddress.setPostalcode(userinfo.getWsrUserAddressList().get(i).getPostalcode());
				wsrUserAddress.setEmail(userinfo.getWsrUserAddressList().get(i).getEmail());
				wsrUserAddress.setActiveflag(userinfo.getWsrUserAddressList().get(i).isActiveflag());
				wsrUserAddress.setCreatedate(userinfo.getWsrUserAccount().getCreatedate());
				wsrUserAddress.setUpdatedate(userinfo.getWsrUserAccount().getUpdatedate());
				wsrUserAddress.setInactivedate(userinfo.getWsrUserAccount().getInactivedate());

				wsrUserAddress.setWsrUserAccount(wsrUserAccount);
				wsrUsrAddList.add(wsrUserAddress);

				log.info("WSR_USER_ADDRESS TABLE POPULATED ");
			}
			wsrUserAccount.setWsrUserAddress(wsrUsrAddList);

			List<WsrUserGroupType> wsrUserGroupTypeList = new ArrayList<WsrUserGroupType>();
			WsrUserGroupType wsrUserGroupType = null;

			List<WsrUserGroupXref> wsrUsrGrpRefList = new ArrayList<WsrUserGroupXref>();
			WsrUserGroupXref wsrUserGroupRef = null;

			List<WsrUserInGroup> wsrUsrInGrpList = new ArrayList<WsrUserInGroup>();
			WsrUserInGroup wsrUserInGroup = null;

			for (int i = 0; i < userinfo.getWsrUserGroupTypeList().size(); i++) {
				wsrUserGroupType = new WsrUserGroupType();

				// *********************************************************
				// Populate wsr_user_group_type DB : 3rd table
				wsrUserGroupType.setGroup_name(userinfo.getWsrUserGroupTypeList().get(i).getGroup_name());
				wsrUserGroupType.setWsrUserAccount(wsrUserAccount);
				wsrUserGroupTypeList.add(wsrUserGroupType);

				log.info("WSR_USER_GRP_TYPE TABLE POPULATED ");

				// *********************************************************
				// populate wsr_user_group_ref DB : 4th table
				wsrUserGroupRef = new WsrUserGroupXref();

				wsrUserGroupRef.setInsert_time(dateformat.format(createdate));
				wsrUserGroupRef.setWsrUserGrpType(wsrUserGroupType);
				wsrUsrGrpRefList.add(wsrUserGroupRef);
				wsrUserGroupType.setWsrUserGroupXref(wsrUserGroupRef);

				log.info("WSR_USER_GRP_REF TABLE POPULATED ");

				// *********************************************************
				// Populate wsr_user_in_group DB : 5th table

				wsrUserInGroup = new WsrUserInGroup();

				WsrUserGroupType grpType = userinfo.getWsrUserGroupTypeList().get(i);
				if (grpType.getWsrUserGroupXref() != null) {
					WsrUserGroupXref grpRef = grpType.getWsrUserGroupXref();
					if (grpRef != null) {
						WsrUserInGroup inGrp = grpRef.getWsrUserInGroup();
						if (inGrp != null) {

							wsrUserInGroup.setDelete_time("");// ToDO
							wsrUserInGroup.setInsert_time(dateformat.format(createdate));
							log.info("");
							log.info("CREATE GROUP_ADMIN : " + inGrp.isGroup_admin());
							wsrUserInGroup.setGroup_admin(inGrp.isGroup_admin());// ToDO
							log.info("");

						}
					}
				}

				log.info("WSR_USER_IN_GRP TABLE POPULATED ");
				log.info("");

				wsrUserInGroup.setWsrUserAccount(wsrUserAccount);
				wsrUserInGroup.setWsrUserGrpRef(wsrUserGroupRef);

				wsrUserGroupRef.setWsrUserInGroup(wsrUserInGroup);
				// wsrUserGroupRef.setWsrUserInGroup(wsrUsrInGrpList);

				wsrUsrInGrpList.add(wsrUserInGroup);
			}
			wsrUserAccount.setWsrUserInGroup(wsrUsrInGrpList);

			wsrUserAccount.setWsrUserGroupType(wsrUserGroupTypeList);

			log.info("ALL THE TABLES POPULATED SUCESSFULLY");

			log.info("============================================");

			WsrUserAccount wsrUserAct = wsrUserAccountService.registerNewUser(wsrUserAccount);
			log.info("============================================");
			log.info(" wsrUserAct :  " + wsrUserAct);
			log.info("============================================");

			topicProducer.sendCreateUserAccountResponseMessage(wsrUserAct, null, null);

			registeredUser = ResponseEntity.ok().body(wsrUserAct);

		}

		return registeredUser;

	}

	@CacheEvict(value = "users", allEntries = true)
	@GetMapping(value = "/{phoneNo}", produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<WsrUserAccount> getUserAccount(@PathVariable String phoneNo)
			throws UserAccountNotFoundException {

		log.info(" GET User Account for the Phone No : " + phoneNo);
		ResponseEntity<WsrUserAccount> registeredUser = null;
		List<WsrUserAccount> users = wsrUserAccountService.getUsers();

		int noOfUsersAvil = users.size();
		log.info("");
		log.info("USERINFO LIST  SIZE : " + noOfUsersAvil);
		log.info("");
		int alreadyAvialblePhoneNo = 0;

		if (noOfUsersAvil == 0) {
			throw new UserAccountNotFoundException(Integer.valueOf(phoneNo));
		} else {

			for (WsrUserAccount usr : users) {
				alreadyAvialblePhoneNo++;
				log.info(" User PhoneNO : " + phoneNo + " , PHONE NO In DB : " + usr.getPhone());
				if (usr.getPhone().equalsIgnoreCase(phoneNo)) {
					log.info("Searcing PhoneNo Found for the Requested User");
					WsrUserAccount userAccount = wsrUserAccountService.getUserById(usr.getUserid());
					registeredUser = ResponseEntity.ok().body(userAccount);
					topicProducer.sendRetriveUserAccountResponseMessage(userAccount, null, null);
					break;
				} else {
					if (alreadyAvialblePhoneNo == noOfUsersAvil) {
						log.info("Searcing PhoneNo is NoT Found for the Requested User");
						
						//topicProducer.sendRetriveUserAccountResponseMessage(null,
							//	"Searcing PhoneNo is NoT Found for the Requested User");// TODO
						
						topicProducer.sendRetriveUserAccountResponseMessage(null,
								new RegistrationError(404, "Searcing PhoneNo " + phoneNo + " is NoT Found for the Requested User"), null);
								
								
						throw new UserAccountNotFoundException(Integer.valueOf(phoneNo));

					}
				}
			}
			return registeredUser;
		}

	}

	@CacheEvict(value = "users", allEntries = true)
	@GetMapping(value = "/all", produces = MediaType.APPLICATION_JSON_VALUE)
	public List<WsrUserAccount> getUsers() {
		log.info(" - Get All User Accounts --.");

		if (wsrUserAccountService.getUsers().size() == 0) {
			topicProducer.sendRetriveUserAccountResponseMessage(null,null,
					"Unable to fetche ALL Users , as no registred user available");// TODO
			throw new UserAccountNotFoundException("Unable to fetche ALL Users , as no registred user available ");
		} else {
			List<WsrUserAccount> userAccounts = wsrUserAccountService.getUsers();
			
			topicProducer.sendRetriveAllUserAccountResponseMessage(userAccounts);
			
			return userAccounts;
		}
	}

	@CacheEvict(value = "users", allEntries = true)
	@DeleteMapping(value = "/{phoneNo}")
	public ResponseEntity<String> deleteUserAccount(@PathVariable String phoneNo) throws UserAccountNotFoundException {

		log.info(" Delete Account Phone No : " + phoneNo);
		String str = null;
		List<WsrUserAccount> users = wsrUserAccountService.getUsers();

		int noOfUsersAvil = users.size();
		log.info("");
		log.info("USERINFO LIST  SIZE : " + noOfUsersAvil);
		log.info("");
		int alreadyAvialblePhoneNo = 0;

		if (noOfUsersAvil == 0) {
			topicProducer.sendDeleteUserAccountResponseMessage(
					"Unable to delete the phone no : ".concat(phoneNo) + ", as the user is not a registred user",
					HttpStatus.OK);
			throw new UserAccountNotFoundException(
					"Unable to delete the phone no : ".concat(phoneNo) + ", as the user is not a registred user");
		}

		else {

			for (WsrUserAccount usr : users) {
				alreadyAvialblePhoneNo++;
				if (usr.getPhone().equalsIgnoreCase(phoneNo)) {
					log.info("MATCH phoneNo to delete with userid : " + usr.getUserid());
					wsrUserAccountService.deleteUserById(usr.getUserid());
					log.info(" Successfully Deleted User Account " + phoneNo);

					topicProducer.sendDeleteUserAccountResponseMessage(
							"Sucessfully Deleted the registered User Account ".concat(phoneNo), HttpStatus.OK);

					return new ResponseEntity<String>(
							"Sucessfully Deleted the registered User Account ".concat(phoneNo), HttpStatus.OK);
				} else {
					if (alreadyAvialblePhoneNo == noOfUsersAvil) {
						log.info("Delete PhoneNo is NoT Found for the Requested User");

						topicProducer.sendDeleteUserAccountResponseMessage(
								"No Registered User Account Available to delete ", HttpStatus.OK);

						return new ResponseEntity<String>("No Registered User Account Available to delete ",
								HttpStatus.OK);
					}
				}
			}
			return new ResponseEntity<String>(str, HttpStatus.OK);
		}

	}

	/*
	@CacheEvict(value = "users", allEntries = true)
	@DeleteMapping(value = "/deleteall")
	public ResponseEntity<String> deleteAllUser() {
		if (wsrUserAccountService.getUsers().size() == 0) {
			throw new UserAccountNotFoundException("Unable to Delete the Users , as no user available");
		} else {
			wsrUserAccountService.deleteAllUser();
			log.info("Deleted all User Accounts");
			return new ResponseEntity<String>("Sucessfully Deleted ALL Registred Users", HttpStatus.OK);
		}
	}
	*/
	
	// @CachePut(value = "users", key = "#userid")
	@CacheEvict(value = "users", allEntries = true)
	@PutMapping(value = "/{phoneNo}", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<WsrUserAccount> updateUserAccount(@RequestBody UserInfo userinfo,
			@PathVariable String phoneNo) throws UserAccountNotFoundException {

		log.info(" UPDATING USER Account Phone No : " + phoneNo);
		boolean userAvailable = true;
		int alreadyAvialblePhoneNo = 0;
		ResponseEntity<WsrUserAccount> response = null;

		List<WsrUserAccount> userAccounts = null;

		int totalAvalUserAccountNos = wsrUserAccountService.getUsers().size();

		if (totalAvalUserAccountNos == 0) {
			log.info("PhoneNo : " + phoneNo + "is NoT Found for the Requested User");
			throw new UserAccountNotFoundException(Integer.valueOf(phoneNo));
		} else {
			userAccounts = wsrUserAccountService.getUsers();
			for (WsrUserAccount user : userAccounts) {
				alreadyAvialblePhoneNo++;
				if (user.getPhone().equalsIgnoreCase(phoneNo)) {
					log.info(" Requested User PhoneNO : " + phoneNo + " , PHONE NO In DB : " + user.getPhone());

					return updateDB(user, userinfo, response);
				} else {
					if (alreadyAvialblePhoneNo == totalAvalUserAccountNos) {
						log.info("PhoneNo : " + phoneNo + "is NoT Found for the Requested User");

						topicProducer.sendUpdateUserAccountResponseMessage(null, 
								new RegistrationError(404,"PhoneNo: " + phoneNo + " is NoT Found for the Requested User"),
								null);
						
						throw new UserAccountNotFoundException(Integer.valueOf(phoneNo));
					}
				}

			}

		}

		return response;
	}

	private ResponseEntity<WsrUserAccount> updateDB(WsrUserAccount user, UserInfo userinfo,
			ResponseEntity<WsrUserAccount> response) {

		// 1st table update: wsr_user_account
		WsrUserAccount wsrUserAccount = wsrUserAccountService.getUserById(user.getUserid());

		wsrUserAccount.setUsertype(userinfo.getWsrUserAccount().getUsertype());

		wsrUserAccount.setFirstname(userinfo.getWsrUserAccount().getFirstname());
		wsrUserAccount.setLastname(userinfo.getWsrUserAccount().getLastname());
		wsrUserAccount.setEmail(userinfo.getWsrUserAccount().getEmail());
		wsrUserAccount.setPassword(userinfo.getWsrUserAccount().getPassword());

		// wsrUserAccount.setPhone(updateuser.getPhone());
		wsrUserAccount.setGender(userinfo.getWsrUserAccount().getGender());
		wsrUserAccount.setDob(userinfo.getWsrUserAccount().getDob());

		DateFormat dateformat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		// dateformat.setTimeZone(TimeZone.getTimeZone("UTC"));

		Date updatedate = new Date();

		wsrUserAccount.setUpdatedate(dateformat.format(updatedate));

		log.info("WSR_USER_ACCOUNT TABLE UPDATED");

		// 2nd table update: wsr_user_address
		for (int i = 0; i < wsrUserAccount.getWsrUserAddress().size(); i++) {
			for (int j = 0; j < userinfo.getWsrUserAddressList().size(); j++) {

				wsrUserAccount.getWsrUserAddress().get(i)
						.setAddressline1(userinfo.getWsrUserAddressList().get(i).getAddressline1());
				wsrUserAccount.getWsrUserAddress().get(i)
						.setAddressline2(userinfo.getWsrUserAddressList().get(i).getAddressline2());
				wsrUserAccount.getWsrUserAddress().get(i).setPhone(userinfo.getWsrUserAddressList().get(i).getPhone());
				wsrUserAccount.getWsrUserAddress().get(i).setCity(userinfo.getWsrUserAddressList().get(i).getCity());
				wsrUserAccount.getWsrUserAddress().get(i).setState(userinfo.getWsrUserAddressList().get(i).getState());
				wsrUserAccount.getWsrUserAddress().get(i)
						.setCountry(userinfo.getWsrUserAddressList().get(i).getCountry());
				wsrUserAccount.getWsrUserAddress().get(i)
						.setPostalcode(userinfo.getWsrUserAddressList().get(i).getPostalcode());
				wsrUserAccount.getWsrUserAddress().get(i).setEmail(userinfo.getWsrUserAddressList().get(i).getEmail());
				wsrUserAccount.getWsrUserAddress().get(i)
						.setAddresstype(userinfo.getWsrUserAddressList().get(i).getAddresstype());
				wsrUserAccount.getWsrUserAddress().get(i).setUpdatedate(dateformat.format(updatedate));

			}
		}
		log.info("WSR_USER_ADDRESS TABLE UPDATED");

		// 3th table update : wsr_user_group_type
		for (int i = 0; i < wsrUserAccount.getWsrUserGroupType().size(); i++) {
			log.info("GroupName : " + wsrUserAccount.getWsrUserGroupType().get(i).getGroup_name());
			log.info("GroupTypeID : " + wsrUserAccount.getWsrUserGroupType().get(i).getUser_group_type_id());

			for (int j = 0; j < userinfo.getWsrUserGroupTypeList().size(); j++) {
				if (userinfo.getWsrUserGroupTypeList().get(j).getGroup_name() != null
						& !userinfo.getWsrUserGroupTypeList().get(j).getGroup_name().isEmpty()) {

					log.info("GROUP_NAME TO UPDATE : " + userinfo.getWsrUserGroupTypeList().get(j).getGroup_name());

					wsrUserAccount.getWsrUserGroupType().get(i)
							.setGroup_name(userinfo.getWsrUserGroupTypeList().get(i).getGroup_name());
				}
			}

		}
		log.info("WSR_USER_GROUP_TYPE TABLE UPDATED");

		// 4th table update : wsr_user_group_ref
		for (int i = 0; i < wsrUserAccount.getWsrUserGroupType().size(); i++) {
			log.info("User_Grp_ID : "
					+ wsrUserAccount.getWsrUserGroupType().get(i).getWsrUserGroupXref().getUser_group_id());
			log.info("UserGrp_TYPE_ID : " + wsrUserAccount.getWsrUserGroupType().get(i).getWsrUserGroupXref()
					.getWsrUserGrpType().getUser_group_type_id());
		}
		log.info("WSR_USER_GROUP_REF TABLE UPDATED");

		// 5th table update : wsr_user_in_group
		for (int i = 0; i < wsrUserAccount.getWsrUserInGroup().size(); i++) {
			log.info("UserInGrpID : " + wsrUserAccount.getWsrUserInGroup().get(i).getUser_in_group_id());

		}
		log.info("WSR_USER_IN_GROUP TABLE UPDATED");

		WsrUserAccount userAccount = wsrUserAccountService.updateUser(wsrUserAccount);
		log.info("");
		log.info(" ***** ALL THE TABLES UPDATED SUCCESSFULLY ******");

		if (userAccount != null) {
			response = ResponseEntity.ok().body(userAccount);

			topicProducer.sendUpdateUserAccountResponseMessage(userAccount, null , null);

			return response;
			// break;
		} else {
			topicProducer.sendUpdateUserAccountResponseMessage(null, 
					new RegistrationError(404, "Not able to update the User Account Information"), null);
			throw new UserAccountNotFoundException(userinfo.getWsrUserAccount().getUserid());

		}

	}

}
