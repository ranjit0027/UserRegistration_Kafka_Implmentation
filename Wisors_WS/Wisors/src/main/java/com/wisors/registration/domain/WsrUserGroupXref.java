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
 * @author Ranjit Sharma ,Wisors INC, USA
 * @since @11-04-2020
 * @version 1.0
 */

@Component
@Entity
@Table(name = "wsr_user_group_ref")
@DynamicInsert
@DynamicUpdate
public class WsrUserGroupXref implements Serializable {

	private static final long serialVersionUID = 3781962238148645461L;

	@Id
	@GeneratedValue(strategy = GenerationType.AUTO)
	@Column(name = "user_group_id")
	private int user_group_id;

	@Column(name = "insert_time")
	private String insert_time;

	@JsonIgnoreProperties({ "hibernateLazyInitializer", "handler" })
	@OneToOne(fetch = FetchType.LAZY, cascade = CascadeType.ALL)
	@JoinColumn(name = "user_group_type_id", nullable = false)
	@JsonBackReference(value = "wsrUserGrpType")
	private WsrUserGroupType wsrUserGrpType;

	@JsonIgnoreProperties({ "hibernateLazyInitializer", "handler" })
	@OneToOne(fetch = FetchType.LAZY, cascade = CascadeType.ALL, mappedBy = "wsrUserGrpRef")
	// @JsonIdentityInfo(generator = ObjectIdGenerators.IntSequenceGenerator.class)
	private WsrUserInGroup wsrUserInGroup;

	public int getUser_group_id() {
		return user_group_id;
	}

	public void setUser_group_id(int user_group_id) {
		this.user_group_id = user_group_id;
	}

	public String getInsert_time() {
		return insert_time;
	}

	public void setInsert_time(String insert_time) {
		this.insert_time = insert_time;

	}

	public WsrUserGroupType getWsrUserGrpType() {
		return wsrUserGrpType;
	}

	public void setWsrUserGrpType(WsrUserGroupType wsrUserGrpType) {
		this.wsrUserGrpType = wsrUserGrpType;
	}

	public WsrUserInGroup getWsrUserInGroup() {
		return wsrUserInGroup;
	}

	public void setWsrUserInGroup(WsrUserInGroup wsrUserInGroup) {
		this.wsrUserInGroup = wsrUserInGroup;
	}

}
