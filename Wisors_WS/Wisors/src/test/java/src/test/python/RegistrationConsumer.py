# -*- coding: utf-8 -*-
"""
Created on Thu Mar  5 11:41:04 2020

@author: ranjit
"""

import requests

import json

from time import sleep
from json import dumps
from kafka import KafkaProducer
from kafka import KafkaConsumer
from json import loads



def pushKafkaMessage():
    print("Push kafka Message")
    
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))
    print("&&&&&&")
    for e in range(1):
        print("Sending data into Kafka")
        kafkadata = {
	"wsrUserAccount": {
		"usertype": "admin",
		"firstname": "Manjit",
		"lastname": "Barman",
		"phone": "8888",
		"email": "mbarman@gmail.com",
		"password": "Weekly",
		"activeflag": True,
		"dob": "2000-12-12",
		"gender": "Male"
	},


	"wsrUserAddressList": [{
			"addresstype": "Home",
			"addressline1": "Boston1",
			"addressline2": "Boston2",
			"phone": "8888",
			"city": "Boston",
			"state": "MA",
			"country": "USA",
			"postalcode": "111111",
			"email": "mbarman@gmail.com",
			"activeflag": True
		},
		{
			"addresstype": "Office",
			"addressline1": "Middletown1",
			"addressline2": "Middletown2",
			"phone": "8888",
			"city": "Delaware",
			"state": "DEL",
			"country": "USA",
			"postalcode": "222222",
			"email": "manjitb2@gmail.com",
			"activeflag": True
		}
	],

	"wsrUserGroupTypeList": [
		{
			"group_name": "M_B_Education",
			"wsrUserGroupXref": 
				{
					"wsrUserInGroup": {
						"group_admin": False
					}
				}
		},
		
		{
			"group_name": "M_B_Healthcare",
			"wsrUserGroupXref": 
			{
					"wsrUserInGroup": {
						"group_admin": True
				}
			}
		}

	]
}

    json_msg = json.dumps(kafkadata)
    #producer.send('createRegistration', json_msg)

    producer.send('createRegistration', value=kafkadata)
    sleep(5)
    print("DONE")
	
	
    
def createKafkaMessageConsumer():
    
    print("")
    
    consumer = KafkaConsumer(
    'createRegistrationResponse',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='my-group',
     value_deserializer=lambda x: loads(x.decode('utf-8')))
    
    for message in consumer:
        print()
        message = message.value
        print("REceived Message == >: " , message) 
        
		 #collection.insert_one(message)
		 #print('{} added to {}'.format(message, collection))
	
	 

    
def getKafkaMessage():
    print("Get KAfka Message ")
    
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))
        
     
    producer.send('retriveRegistration', 8888)
    sleep(5)
    print("DONE")
    

def deleteKafkaMessage():
    
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))
        
     
    producer.send('deleteRegistration', 8888)
    sleep(5)
    print("DONE")
    

    
        
def putKafkaMessage():
    print("Put kafka Message")
    
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))
    print("&&&&&&")
    for e in range(1):
        print("Sending data into Kafka")
        kafkadata = {
	"wsrUserAccount": {
		"usertype": "admin",
		"firstname": "Manjit",
		"lastname": "Barman",
		"phone": "8888",
		"email": "mbarman@gmail.com",
		"password": "Weekly",
		"activeflag": True,
		"dob": "2000-12-12",
		"gender": "Male"
		
	},


	"wsrUserAddressList": [{
			"addresstype": "Home",
			"addressline1": "NYC1",
			"addressline2": "NYC2",
			"phone": "8888",
			"city": "NYC",
			"state": "NYC",
			"country": "USA",
			"postalcode": "22222",
			"email": "m2@gmail.com",
			"activeflag": True
		},
		{
			"addresstype": "Office",
			"addressline1": "Cal1",
			"addressline2": "Cal2",
			"phone": "8888",
			"city": "CALIFORNIA",
			"state": "CAL",
			"country": "USA",
			"postalcode": "333333",
			"email": "m3@gmail.com",
			"activeflag": True
		}
	],

	"wsrUserGroupTypeList": [
		{
			"group_name": "M_B_Research",
			"wsrUserGroupXref": 
				{
					"wsrUserInGroup": {
						"group_admin": False
					}
				}
		},
		
		{
			"group_name": "M_B_STEM",
			"wsrUserGroupXref": 
			{
					"wsrUserInGroup": {
						"group_admin": True
				}
			}
		}

	]
}
    producer.send('updateRegistration', value=kafkadata)
    sleep(5)
    print("DONE")
    
    
    
    
    
  
def postRegistraion():
    
    print("postRegistraion from Client")
    
    url = 'http://localhost:8080/user/registration/create'
    #url = 'http://localhost:8080/user/kafka/publishMsg'
    
    print ("url : " , url)

    headers = {
        'cache-control': "no-cache",
    }
    
    
    response = requests.post(url, json = {"usertype": "admin", 	
  "firstname": "ranjit",
  "lastname": "sharma",
  "addressline1": "marthahalli1",
  "addressline2": "marthahalli2",
  "city": "Bangalore",
  "state": "KA",
  "pin": "560067",
  "country": "INDIA",
  "phone": "2000",
  "email": "ranjit0027@gmail.com",
  "subscriptionlevel": "Weekly",
  "dob": "1985-12-12",
  "gender": "Male",
  "groupid" : "2",
  "addresstype":"home"}
   , headers=headers)
    
    responseStatusCode = response.status_code
    print(" responseStatusCode : " , responseStatusCode)
    
    print("-----------------------------------")
    print("")
    
    data = response.text
    print(" RESPONSE DATA : " , data)
    print("")
    print("-----------------------------------")
    
    

def putRegistraion():
    
    print("putRegistraion from Client")
    
    #url="http://localhost:8080/user/registration/update/phone?primaryPhone=2000"
    url="http://localhost:8080/user/registration/update/phone/8888"
    
    print ("url : " , url)

    headers = {
        'cache-control': "no-cache",
    }
    
    
   #data = {"firstname":"ranjit","lastname":"sharma","state":"KA","country":"India","email":"ranjit@gmail.com","phone":"9999999","streetnumber":"2524","streetname":"belathur","city":"Bangalore","servicename":"HealthCare","plan":"Wisor Basic","cardnumber":"88888888","nameoncard":"Ranjitsharma","expirydate":"2030-12-31","servicedescription":"Diabetic Test","billingaddress":"Whitefiled,Bangalore"}
    
    #response = requests.request("GET", url, headers=headers)
    response = requests.put(url, json = {"usertype": "admin", 	
  "firstname": "ranjit",
  "lastname": "sharma",
  "addressline1": "marthahalli1",
  "addressline2": "marthahalli2",
  "city": "Bangalore",
  "state": "KA",
  "pin": "560067",
  "country": "INDIA",
  "phone": "2000",
  "email": "ranjit0027@gmail.com",
  "subscriptionlevel": "Monthly",
  "dob": "1985-12-12",
  "gender": "Male",
  "groupid" : "1",
  "addresstype":"Office"} 
    , headers=headers)
    
    responseStatusCode = response.status_code
    print(" responseStatusCode : " , responseStatusCode)
    
    print("-----------------------------------")
    print("")
    
    data = response.text
    print(" RESPONSE DATA : " , data)
    
    
    


def deleteOperations():
    print("Delete operations ...")
    #url="http://localhost:8080/user/registration/delete/phone?primaryPhone=2000"
    url="http://localhost:8080/user/registration/delete/phone/2222"
    
    
    headers = {
        'cache-control': "no-cache",
    }
    
    response = requests.delete(url,headers=headers)
    responseStatusCode = response.status_code
    print(" responseStatusCode : " , responseStatusCode)
    
    print("-----------------------------------")
    print("")
    
    data = response.text
    print(" RESPONSE DATA : " , data)
    

def retriveOperations():
    print("Retrive operations ...")
    url="http://localhost:8080/user/phone?primaryPhone=333333"
    #url="http://localhost:8080/user/phone/2222"
    
    
    headers = {
        'cache-control': "no-cache",
    }
    
    response = requests.get(url,headers=headers)
    responseStatusCode = response.status_code
    print(" responseStatusCode : " , responseStatusCode)
    
    print("-----------------------------------")
    print("")
    
    data = response.text
    print(" RESPONSE DATA : " , data)
    

def retriveOperations_invalidPhNo():
    print("retriveOperations_invalid  ...")
    url="http://localhost:8080/user/phone?primaryPhone=3000"
    
    headers = {
        'cache-control': "no-cache",
    }
    
    response = requests.get(url,headers=headers)
    responseStatusCode = response.status_code
    print(" responseStatusCode : " , responseStatusCode)
    
    print("-----------------------------------")
    print("")
    
    data = response.text
    print(" RESPONSE DATA : " , data)


def putRegistraion_invalidPhNo():
    
    print("putRegistraion_invalidPhNo from Client")
    
    url="http://localhost:8080/user/registration/update/phone?primaryPhone=3000"
    print ("url : " , url)

    headers = {
        'cache-control': "no-cache",
    }
    
    
    response = requests.put(url, json = {"usertype": "admin", 	
  "firstname": "ranjit",
  "lastname": "sharma",
  "addressline1": "marthahalli1",
  "addressline2": "marthahalli2",
  "city": "Bangalore",
  "state": "KA",
  "pin": "560067",
  "country": "INDIA",
  "email": "ranjit0027@gmail.com",
  "subscriptionlevel": "Monthly",
  "dob": "1985-12-12",
  "gender": "Male",
  "groupid" : "1",
  "addresstype":"Office"} 
    , headers=headers)
    
    responseStatusCode = response.status_code
    print(" responseStatusCode : " , responseStatusCode)
    
    print("-----------------------------------")
    print("")
    
    data = response.text
    print(" RESPONSE DATA : " , data)


def deleteOperations_invalidPhNo():
    print("deleteOperations_invalidPhNo  ...")
    url="http://localhost:8080/user/registration/delete/phone?primaryPhone=88888"
    
    headers = {
        'cache-control': "no-cache",
    }
    
    response = requests.delete(url,headers=headers)
    responseStatusCode = response.status_code
    print(" responseStatusCode : " , responseStatusCode)
    
    print("-----------------------------------")
    print("")
    
    data = response.text
    print(" RESPONSE DATA : " , data)
    
   
def main():
    
    print("**************")
    
    pushKafkaMessage()
    
    #putKafkaMessage()
    
    #getKafkaMessage()
    
    #deleteKafkaMessage()
    
    
    #postRegistraion()
    
    #retriveOperations()
    
   # putRegistraion()
    
    #deleteOperations()
    
    #retriveOperations_invalidPhNo()
    
    #putRegistraion_invalidPhNo()
    
    #deleteOperations_invalidPhNo()


    
if __name__ == "__main__": main()