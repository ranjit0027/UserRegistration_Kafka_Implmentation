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
import javax.persistence.OneToMany;
import javax.persistence.OneToOne;
import javax.persistence.Table;

import org.hibernate.annotations.DynamicInsert;
import org.hibernate.annotations.DynamicUpdate;
import org.springframework.stereotype.Component;

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
@Table(name = "wsr_user_account")
@DynamicInsert
@DynamicUpdate
public class WsrUserAccount implements Serializable {

	private static final long serialVersionUID = 5146476428621058629L;

	@Id
	@GeneratedValue(strategy = GenerationType.AUTO)
	@Column(name = "userid")
	private Integer userid;

	@Column(name = "usertype")
	private String usertype;

	@Column(name = "firstname")
	private String firstname;

	@Column(name = "lastname")
	private String lastname;

	@Column(name = "phone")
	private String phone;

	@Column(name = "email")
	private String email;

	@Column(name = "password")
	private String password;

	@Column(name = "activeflag")
	private boolean activeflag;

	@Column(name = "createdate")
	private String createdate;

	@Column(name = "updatedate")
	private String updatedate;

	@Column(name = "inactivedate")
	private String inactivedate;

	@Column(name = "dob")
	private String dob;

	@Column(name = "gender")
	private String gender;

	@JsonIgnoreProperties({ "hibernateLazyInitializer", "handler" })
	@OneToMany(fetch = FetchType.LAZY, cascade = CascadeType.ALL, mappedBy = "wsrUserAccount")
	@Column(nullable = true)
	// @JsonIdentityInfo(generator = ObjectIdGenerators.IntSequenceGenerator.class)
	private List<WsrUserAddress> wsrUserAddress;

	@JsonIgnoreProperties({ "hibernateLazyInitializer", "handler" })
	@OneToMany(fetch = FetchType.LAZY, cascade = CascadeType.ALL, mappedBy = "wsrUserAccount")
	@Column(nullable = true)
	// @JsonIdentityInfo(generator = ObjectIdGenerators.IntSequenceGenerator.class)
	private List<WsrUserInGroup> wsrUserInGroup;

	@JsonIgnoreProperties({ "hibernateLazyInitializer", "handler" })
	@OneToMany(fetch = FetchType.LAZY, cascade = CascadeType.ALL, mappedBy = "wsrUserAccount")
	@Column(nullable = true)
	// @JsonIdentityInfo(generator = ObjectIdGenerators.IntSequenceGenerator.class)
	private List<WsrUserGroupType> wsrUserGroupType;

	public WsrUserAccount() {
		super();
	}

	public Integer getUserid() {
		return userid;
	}

	public void setUserid(Integer userid) {
		this.userid = userid;
	}

	public String getUsertype() {
		return usertype;
	}

	public void setUsertype(String usertype) {
		this.usertype = usertype;
	}

	public String getFirstname() {
		return firstname;
	}

	public void setFirstname(String firstname) {
		this.firstname = firstname;
	}

	public String getLastname() {
		return lastname;
	}

	public void setLastname(String lastname) {
		this.lastname = lastname;
	}

	public String getPhone() {
		return phone;
	}

	public void setPhone(String phone) {
		this.phone = phone;
	}

	public String getEmail() {
		return email;
	}

	public void setEmail(String email) {
		this.email = email;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public boolean isActiveflag() {
		return activeflag;
	}

	public void setActiveflag(boolean activeflag) {
		this.activeflag = activeflag;
	}

	public String getCreatedate() {
		return createdate;
	}

	public void setCreatedate(String createdate) {
		this.createdate = createdate;
	}

	public String getUpdatedate() {
		return updatedate;
	}

	public void setUpdatedate(String updatedate) {
		this.updatedate = updatedate;
	}

	public String getInactivedate() {
		return inactivedate;
	}

	public void setInactivedate(String inactivedate) {
		this.inactivedate = inactivedate;
	}

	public String getDob() {
		return dob;
	}

	public void setDob(String dob) {
		this.dob = dob;
	}

	public String getGender() {
		return gender;
	}

	public void setGender(String gender) {
		this.gender = gender;
	}

	public List<WsrUserAddress> getWsrUserAddress() {
		return wsrUserAddress;
	}

	public void setWsrUserAddress(List<WsrUserAddress> wsrUserAddress) {
		this.wsrUserAddress = wsrUserAddress;
	}

	public List<WsrUserInGroup> getWsrUserInGroup() {
		return wsrUserInGroup;
	}

	public void setWsrUserInGroup(List<WsrUserInGroup> wsrUserInGroup) {
		this.wsrUserInGroup = wsrUserInGroup;
	}

	public List<WsrUserGroupType> getWsrUserGroupType() {
		return wsrUserGroupType;
	}

	public void setWsrUserGroupType(List<WsrUserGroupType> wsrUserGroupType) {
		this.wsrUserGroupType = wsrUserGroupType;
	}

}