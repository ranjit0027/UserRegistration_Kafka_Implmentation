package com.wisors.registration.exception;

/**
 * 
 * @author Ranjit Sharma ,Wisors INC, USA
 * @since @11-04-2020
 * @version 1.0
 */

public class UserAccountNotFoundException extends RuntimeException {

	private static final long serialVersionUID = -3625857624790597393L;

	public UserAccountNotFoundException(int userid) {
		super("Could not find user " + userid);

	}

	public UserAccountNotFoundException(String message) {
		super(message);

	}
}
