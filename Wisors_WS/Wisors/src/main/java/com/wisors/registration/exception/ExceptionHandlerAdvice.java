package com.wisors.registration.exception;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;

/**
 * 
 * @author Ranjit Sharma ,Wisors INC, USA
 * @since @11-04-2020
 * @version 1.0
 */

@ControllerAdvice
public class ExceptionHandlerAdvice {

	@ResponseBody
	@ExceptionHandler(UserAccountNotFoundException.class)
	@ResponseStatus(HttpStatus.NOT_FOUND)
	public ResponseEntity<RegistrationError> userNotFoundHandler(UserAccountNotFoundException ex) {

		RegistrationError error = new RegistrationError(HttpStatus.NOT_FOUND.value(), ex.getMessage());

		return new ResponseEntity<RegistrationError>(error, HttpStatus.NOT_FOUND);
	}

}
