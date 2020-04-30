package com.wisors.registration.service;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.wisors.registration.repository.UserAccountRepository;
import com.wisors.registration.exception.UserAccountNotFoundException;
import com.wisors.registration.controller.UserAccountController;
import com.wisors.registration.domain.WsrUserAccount;

/**
 * 
 * @author Ranjit Sharma ,Wisors INC, USA
 * @since @11-04-2020
 * @version 1.0
 */

@Service
public class UserAccountServiceImpl implements UserAccountService {

	private static final Logger log = LoggerFactory.getLogger(UserAccountServiceImpl.class);

	@Autowired
	UserAccountRepository usrActDao;

	@Autowired
	public UserAccountServiceImpl(UserAccountRepository usrActDao) {
		this.usrActDao = usrActDao;
	}

	@Override
	public WsrUserAccount registerNewUser(WsrUserAccount user) throws UserAccountNotFoundException {

		log.info("*********registerNewUser**************");
		WsrUserAccount userAccount = null;

		try {
			userAccount = usrActDao.save(user);

		} catch (Exception e) {

			throw new UserAccountNotFoundException("Could not find user ");

		}
		return userAccount;

	}

	@Override
	public List<WsrUserAccount> getUsers() {

		return usrActDao.findAll();
	}

	@Override
	public WsrUserAccount getUserById(int userid) throws UserAccountNotFoundException {
		log.info(" Retriving User id : " + userid);
		return usrActDao.findById(userid).orElseThrow(() -> new UserAccountNotFoundException(userid));
		

	}

	@Override
	public WsrUserAccount updateUser(WsrUserAccount user) throws UserAccountNotFoundException {

		log.info("*********Update User**************");

		try {
			return usrActDao.save(user);
		} catch (Exception e) {
			throw new UserAccountNotFoundException(String.valueOf(user.getUserid()));
		}
	}

	@Override
	public void deleteUserById(int userid) {
		usrActDao.deleteById(userid);

	}

	@Override
	public void deleteAllUser() {
		usrActDao.deleteAll();

	}

}
