package com.wisors.registration.repository;

import org.springframework.data.jpa.repository.JpaRepository;

import com.wisors.registration.domain.WsrUserAccount;

/**
 * 
 * @author Ranjit Sharma ,Wisors INC, USA
 * @since @11-04-2020
 * @version 1.0
 */

public interface UserAccountRepository extends JpaRepository<WsrUserAccount, Integer> {

}
