package com.wisors.registration;

import java.util.TimeZone;

public class Test {

	public static void main(String[] args) {
		// TODO Auto-generated method stubs

		TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
		
		//System.out.println("time : " + TimeZone.getTimeZone(ID));
		
		String str = "-6887-";
		System.out.println(str.length());
		
		System.out.println("substring : " + str.substring(0));
		
		
		char c1 = str.charAt(0);
		System.out.println("c1: " + c1);
		
		char c2 = str.charAt(str.length()-1); 
		System.out.println("c2: " + c2);

		System.out.println("str.lastIndexOf(1):  "  + str.indexOf(c1));
		
		System.out.println("str.lastIndexOf(str):  "  + str.indexOf(c2));
		
		

		
		String str2 = str.substring(str.indexOf(str.charAt(1)), str.indexOf(c2));
		
		System.out.println(" str2 : " + str2);
		
		char c3 = '"';
		

		if (c1 == c3) {

			System.out.println("found match char");
		}

		else {
			System.out.println("Done");
		}

	}

}
