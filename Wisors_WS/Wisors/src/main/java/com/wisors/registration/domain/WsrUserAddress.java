package com.wisors.registration.domain;

import java.io.Serializable;
import java.util.Date;
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
import javax.persistence.OneToOne;
import javax.persistence.Table;

import org.hibernate.annotations.DynamicInsert;
import org.hibernate.annotations.DynamicUpdate;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.annotation.JsonBackReference;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * 
 * @author Ranjit Sharma Wisors INC, USA
 * @since @11-04-2020
 * @version 1.0
 */

@Component
@Entity
@Table(name = "wsr_user_address")
@DynamicInsert
@DynamicUpdate
public class WsrUserAddress implements Serializable {

	private static final long serialVersionUID = -1041448190970886121L;

	@Id
	@GeneratedValue(strategy = GenerationType.AUTO)
	@Column(name = "user_address_id")
	private int user_address_id;

	@Column(name = "addresstype")
	private String addresstype;

	@Column(name = "addressline1")
	private String addressline1;

	@Column(name = "addressline2")
	private String addressline2;

	@Column(name = "phone")
	private String phone;

	@Column(name = "city")
	private String city;

	@Column(name = "state")
	private String state;

	@Column(name = "country")
	private String country;

	@Column(name = "postalcode")
	private String postalcode;

	@Column(name = "email")
	private String email;

	@Column(name = "activeflag")
	private boolean Activeflag;

	@Column(name = "createdate")
	private String createdate;

	@Column(name = "updatedate")
	private String updatedate;

	@Column(name = "inactivedate")
	private String inactivedate;

	@JsonIgnoreProperties({ "hibernateLazyInitializer", "handler" })
	@ManyToOne(fetch = FetchType.LAZY, cascade = CascadeType.ALL)
	@JoinColumn(name = "userid", nullable = false)
	@JsonBackReference(value = "wsrUserAccount")
	private WsrUserAccount wsrUserAccount;

	public WsrUserAddress() {
		super();
	}

	public int getUser_address_id() {
		return user_address_id;
	}

	public void setUser_address_id(int user_address_id) {
		this.user_address_id = user_address_id;
	}

	public String getAddresstype() {
		return addresstype;
	}

	public void setAddresstype(String addresstype) {
		this.addresstype = addresstype;
	}

	public String getAddressline1() {
		return addressline1;
	}

	public void setAddressline1(String addressline1) {
		this.addressline1 = addressline1;
	}

	public String getAddressline2() {
		return addressline2;
	}

	public void setAddressline2(String addressline2) {
		this.addressline2 = addressline2;
	}

	public String getPhone() {
		return phone;
	}

	public void setPhone(String phone) {
		this.phone = phone;
	}

	public String getCity() {
		return city;
	}

	public void setCity(String city) {
		this.city = city;
	}

	public String getState() {
		return state;
	}

	public void setState(String state) {
		this.state = state;
	}

	public String getCountry() {
		return country;
	}

	public void setCountry(String country) {
		this.country = country;
	}

	public String getPostalcode() {
		return postalcode;
	}

	public void setPostalcode(String postalcode) {
		this.postalcode = postalcode;
	}

	public String getEmail() {
		return email;
	}

	public void setEmail(String email) {
		this.email = email;
	}

	public boolean isActiveflag() {
		return Activeflag;
	}

	public void setActiveflag(boolean activeflag) {
		Activeflag = activeflag;
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

	public WsrUserAccount getWsrUserAccount() {
		return wsrUserAccount;
	}

	public void setWsrUserAccount(WsrUserAccount wsrUserAccount) {
		this.wsrUserAccount = wsrUserAccount;
	}

}
