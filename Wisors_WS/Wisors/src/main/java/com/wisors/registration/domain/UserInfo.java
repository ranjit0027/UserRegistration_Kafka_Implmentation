package com.wisors.registration.domain;

import java.io.Serializable;
import java.util.List;

/**
 * 
 * @author Ranjit Sharma ,Wisors INC, USA
 * @since @11-04-2020
 * @version 1.0
 */

public class UserInfo implements Serializable {

	private static final long serialVersionUID = 7194704291479460541L;

	private WsrUserAccount wsrUserAccount;

	private List<WsrUserAddress> wsrUserAddressList;

	private List<WsrUserInGroup> wsrUserInGroupList;

	private List<WsrUserGroupType> wsrUserGroupTypeList;

	public UserInfo() {

	}

	public WsrUserAccount getWsrUserAccount() {
		return wsrUserAccount;
	}

	public void setWsrUserAccount(WsrUserAccount wsrUserAccount) {
		this.wsrUserAccount = wsrUserAccount;
	}

	public List<WsrUserAddress> getWsrUserAddressList() {
		return wsrUserAddressList;
	}

	public void setWsrUserAddressList(List<WsrUserAddress> wsrUserAddressList) {
		this.wsrUserAddressList = wsrUserAddressList;
	}

	public List<WsrUserInGroup> getWsrUserInGroupList() {
		return wsrUserInGroupList;
	}

	public void setWsrUserInGroupList(List<WsrUserInGroup> wsrUserInGroupList) {
		this.wsrUserInGroupList = wsrUserInGroupList;
	}

	public List<WsrUserGroupType> getWsrUserGroupTypeList() {
		return wsrUserGroupTypeList;
	}

	public void setWsrUserGroupTypeList(List<WsrUserGroupType> wsrUserGroupTypeList) {
		this.wsrUserGroupTypeList = wsrUserGroupTypeList;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((wsrUserAccount == null) ? 0 : wsrUserAccount.hashCode());
		result = prime * result + ((wsrUserAddressList == null) ? 0 : wsrUserAddressList.hashCode());
		result = prime * result + ((wsrUserGroupTypeList == null) ? 0 : wsrUserGroupTypeList.hashCode());
		result = prime * result + ((wsrUserInGroupList == null) ? 0 : wsrUserInGroupList.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		UserInfo other = (UserInfo) obj;
		if (wsrUserAccount == null) {
			if (other.wsrUserAccount != null)
				return false;
		} else if (!wsrUserAccount.equals(other.wsrUserAccount))
			return false;
		if (wsrUserAddressList == null) {
			if (other.wsrUserAddressList != null)
				return false;
		} else if (!wsrUserAddressList.equals(other.wsrUserAddressList))
			return false;
		if (wsrUserGroupTypeList == null) {
			if (other.wsrUserGroupTypeList != null)
				return false;
		} else if (!wsrUserGroupTypeList.equals(other.wsrUserGroupTypeList))
			return false;
		if (wsrUserInGroupList == null) {
			if (other.wsrUserInGroupList != null)
				return false;
		} else if (!wsrUserInGroupList.equals(other.wsrUserInGroupList))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "UserInfo [wsrUserAccount=" + wsrUserAccount + ", wsrUserAddressList=" + wsrUserAddressList
				+ ", wsrUserInGroupList=" + wsrUserInGroupList + ", wsrUserGroupTypeList=" + wsrUserGroupTypeList + "]";
	}

}
