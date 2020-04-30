package com.wisors.registration.service;

import java.util.List;

import com.wisors.registration.domain.WsrUserAccount;

/**
 * 
 * @author Ranjit Sharma ,Wisors INC, USA
 * @since @11-04-2020
 * @version 1.0
 */

public interface UserAccountService {

	public WsrUserAccount registerNewUser(WsrUserAccount user);

	public List<WsrUserAccount> getUsers();

	public WsrUserAccount getUserById(int userid);

	public WsrUserAccount updateUser(WsrUserAccount user);

	public void deleteAllUser();

	public void deleteUserById(int userid);

}
