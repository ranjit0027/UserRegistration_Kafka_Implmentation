package com.wisors.registration.domain;

import java.io.Serializable;

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
 * @author Ranjit Sharma ,Wisors INC, USA
 * @since @11-04-2020
 * @version 1.0
 */

@Component
@Entity
@Table(name = "wsr_user_in_group")
@DynamicInsert
@DynamicUpdate
public class WsrUserInGroup implements Serializable {

	private static final long serialVersionUID = 7974944114814394686L;

	@Id
	@GeneratedValue(strategy = GenerationType.AUTO)
	@Column(name = "user_in_group_id")
	private int user_in_group_id;

	@Column(name = "delete_time")
	private String delete_time;

	@Column(name = "insert_time")
	private String insert_time;

	@Column(name = "group_admin")
	private boolean group_admin;

	@JsonIgnoreProperties({ "hibernateLazyInitializer", "handler" })
	@ManyToOne(fetch = FetchType.LAZY, cascade = CascadeType.ALL)
	@JoinColumn(name = "userid", nullable = false)
	@JsonBackReference(value = "wsrUserAccount")
	private WsrUserAccount wsrUserAccount;

	@JsonIgnoreProperties({ "hibernateLazyInitializer", "handler" })
	@OneToOne(fetch = FetchType.LAZY, cascade = CascadeType.ALL)
	@JoinColumn(name = "user_group_id", nullable = false)
	@JsonBackReference(value = "wsrUserGrpRef")
	private WsrUserGroupXref wsrUserGrpRef;

	public WsrUserInGroup() {

	}

	public int getUser_in_group_id() {
		return user_in_group_id;
	}

	public void setUser_in_group_id(int user_in_group_id) {
		this.user_in_group_id = user_in_group_id;
	}

	public String getDelete_time() {
		return delete_time;
	}

	public void setDelete_time(String delete_time) {
		this.delete_time = delete_time;
	}

	public String getInsert_time() {
		return insert_time;
	}

	public void setInsert_time(String insert_time) {
		this.insert_time = insert_time;
	}

	public boolean isGroup_admin() {
		return group_admin;
	}

	public void setGroup_admin(boolean group_admin) {
		this.group_admin = group_admin;
	}

	public WsrUserAccount getWsrUserAccount() {
		return wsrUserAccount;
	}

	public void setWsrUserAccount(WsrUserAccount wsrUserAccount) {
		this.wsrUserAccount = wsrUserAccount;
	}

	public WsrUserGroupXref getWsrUserGrpRef() {
		return wsrUserGrpRef;
	}

	public void setWsrUserGrpRef(WsrUserGroupXref wsrUserGrpRef) {
		this.wsrUserGrpRef = wsrUserGrpRef;
	}

}
