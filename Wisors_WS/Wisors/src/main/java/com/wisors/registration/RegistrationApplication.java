package com.wisors.registration;

import java.util.TimeZone;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.web.client.RestTemplate;

/**
 * 
 * @author Ranjit Sharma ,Wisors INC, USA
 * @since @11-04-2020
 * @version 1.0
 */

@EnableCaching
@SpringBootApplication
public class RegistrationApplication {

	private static final Logger log = LoggerFactory.getLogger(RegistrationApplication.class);

	@Bean
	public RestTemplate createRestTemplate() {

		return new RestTemplate();
	}

	@PostConstruct
	public void init() {
		// Setting Spring Boot SetTimeZone
		TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
	}

	public static void main(String[] args) {
		SpringApplication.run(RegistrationApplication.class, args);
		log.info(".... : Started RegistrationApplication  :.....");
	}

}
