package com.wisors.registration.domain;

import java.io.Serializable;
import java.util.List;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import javax.persistence.OneToOne;
import javax.persistence.Table;

import org.hibernate.annotations.DynamicInsert;
import org.hibernate.annotations.DynamicUpdate;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.annotation.JsonBackReference;
import com.fasterxml.jackson.annotation.JsonIdentityInfo;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.ObjectIdGenerators;

/**
 * 
 * @author Ranjit Sharma Wisors INC, USA
 * @since @11-04-2020
 * @version 1.0
 */

@Component
@Entity
@Table(name = "wsr_user_group_type")
@DynamicInsert
@DynamicUpdate
public class WsrUserGroupType implements Serializable {

	private static final long serialVersionUID = -3270051462672899135L;

	@Id
	@GeneratedValue(strategy = GenerationType.AUTO)
	@Column(name = "user_group_type_id")
	private int user_group_type_id;

	@Column(name = "group_name")
	private String group_name;

	@JsonIgnoreProperties({ "hibernateLazyInitializer", "handler" })
	@ManyToOne(fetch = FetchType.LAZY, cascade = CascadeType.ALL)
	@JoinColumn(name = "userid", nullable = false)
	@JsonBackReference(value = "wsrUserAccount")
	private WsrUserAccount wsrUserAccount;

	@JsonIgnoreProperties({ "hibernateLazyInitializer", "handler" })
	@OneToOne(fetch = FetchType.LAZY, cascade = CascadeType.ALL, mappedBy = "wsrUserGrpType")
	// @JsonIdentityInfo(generator = ObjectIdGenerators.IntSequenceGenerator.class)
	private WsrUserGroupXref wsrUserGroupXref;

	public int getUser_group_type_id() {
		return user_group_type_id;
	}

	public void setUser_group_type_id(int user_group_type_id) {

		this.user_group_type_id = user_group_type_id;
	}

	public String getGroup_name() {
		return group_name;
	}

	public void setGroup_name(String group_name) {
		this.group_name = group_name;
	}

	public WsrUserAccount getWsrUserAccount() {
		return wsrUserAccount;
	}

	public void setWsrUserAccount(WsrUserAccount wsrUserAccount) {
		this.wsrUserAccount = wsrUserAccount;
	}

	public WsrUserGroupXref getWsrUserGroupXref() {
		return wsrUserGroupXref;
	}

	public void setWsrUserGroupXref(WsrUserGroupXref wsrUserGroupXref) {
		this.wsrUserGroupXref = wsrUserGroupXref;
	}

}
