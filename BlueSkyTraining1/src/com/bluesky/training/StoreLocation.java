package com.bluesky.training;

public class StoreLocation {

	public int stloc_id;
	public String identifier;
	public String phone;
	public String fax;
	public String address1;
	public String address2;
	public String address3;
	public String city;
	public String stateProvince;
	public String country;
	public String zipCode;
	public String active;
	
	StoreLocation(int stloc_id,String identifier,String phone,String fax,String address1,String address2,String address3,
			String city,String stateProvince,String country,String zipCode,String active){
		this.stloc_id=stloc_id;
		this.identifier=identifier;
		this.phone=phone;
		this.fax=fax;
		this.address1=address1;
		this.address2=address2;
		this.address3=address3;
		this.city=city;
		this.stateProvince=stateProvince;
		this.country=country;
		this.zipCode=zipCode;
		this.active=active;
	}
	
	public boolean equals(Object object) {
		
		if(object instanceof StoreLocation) {
			
			StoreLocation object1=(StoreLocation)object;
			if(stloc_id==object1.stloc_id && identifier.equals(object1.identifier) && 
					phone.equals(object1.phone) && fax.equals(object1.fax) && 
					address1.equals(object1.address1) && address2.equals(object1.address2) && 
					address3.equals(object1.address3) && city.equals(object1.city) &&
					stateProvince.equals(object1.stateProvince) && country.equals(object1.country) &&
					zipCode.equals(object1.zipCode) && active.equals(object1.active)) {
				return true;
			}
		}
		return false;
	}

	public static void main(String[] args) {
		
		StoreLocation object=new StoreLocation(12752,"Kay Jewelers 1452","(910) 794-3280","","3500 Oleander Dr.","","","Wilmington","NC","USA","28403","1");
		StoreLocation object1=new StoreLocation(12752,"Kay Jewelers 1452","(910) 794-3280","","3500 Oleander Dr.","","","Wilmington","NC","USA","28403","1");
		//StoreLocation object1=new StoreLocation(12751,"Kay Jewelers 1210","(860) 644-7207","","194 Buckland Hills Dr","","","Manchester","CT","USA","06042","1");
		
		if(object.equals(object1)) {
			
			System.out.println("Both are equal");
		}else {
			System.out.println("Both are Not equal");
		}

	}
}
