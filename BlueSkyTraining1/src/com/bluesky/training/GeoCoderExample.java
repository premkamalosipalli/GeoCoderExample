package com.bluesky.training;

import java.io.*;
import java.net.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;

import org.json.*;
import java.util.*;
import java.util.logging.*;

import org.apache.commons.csv.*;

public class GeoCoderExample {

	private static final Logger logger = Logger.getLogger(GeoCoderExample.class.getName());
	Dbconnection dbconn = new Dbconnection();
	List<String[]> writerList = new ArrayList<String[]>();
	private String api;
	private String benchmark;

	public String getApi() {
		return api;
	}

	public void setApi(String api) {
		this.api = api;
	}

	public String getBenchmark() {
		return benchmark;
	}

	public void setBenchmark(String benchmark) {
		this.benchmark = benchmark;
	}

	public void geoCodeFile(String sourceFile, String destinationFile) throws ClassNotFoundException, SQLException {
		try {
			Reader reader = Files.newBufferedReader(Paths.get(sourceFile));
			CSVParser csvparser = new CSVParser(reader, CSVFormat.DEFAULT.withHeader("STLOC_ID", "IDENTIFIER", "PHONE",
					"FAX", "ADDRESS1", "ADDRESS2", "ADDRESS3", "CITY", "STATE", "COUNTRY", "ZIPCODE", "ACTIVE"));
			logger.info("File Generation is in Progress.....");
			for (CSVRecord line : csvparser) {
				// Don't geocode the header line; Add the two additional header columns in the
				// writerList.
				if (line.getRecordNumber() == 1) {
					String[] headers = { line.get("STLOC_ID"), line.get("IDENTIFIER"), line.get("PHONE"),
							line.get("FAX"), line.get("ADDRESS1"), line.get("ADDRESS2"), line.get("ADDRESS3"),
							line.get("CITY"), line.get("STATE"), line.get("COUNTRY"), line.get("ZIPCODE"),
							line.get("ACTIVE"), "Longitude", "Latitude" };
					writerList.add(headers);
					continue;
				}
				Address address = new Address();
				address.setStloc_id(line.get("STLOC_ID"));
				address.setIdentifier(line.get("IDENTIFIER"));
				address.setPhone(line.get("PHONE"));
				address.setFax(line.get("FAX"));
				address.setAddress1(line.get("ADDRESS1"));
				address.setAddress2(line.get("ADDRESS2"));
				address.setAddress3(line.get("ADDRESS3"));
				address.setCity(line.get("CITY"));
				address.setStateProvince(line.get("STATE"));
				address.setCountry(line.get("COUNTRY"));
				address.setZipCode(line.get("ZIPCODE"));
				address.setActive(line.get("ACTIVE"));

				address = geoCodeAddress(this.getApi(), this.getBenchmark(), address);
			}
			csvparser.close();
			BufferedWriter writer = Files.newBufferedWriter(Paths.get(destinationFile));
			CSVPrinter csvprinter = new CSVPrinter(writer, CSVFormat.DEFAULT);
			csvprinter.printRecords(writerList);
			csvprinter.close();
			logger.info("Data Has Entered into the H2 DataBase and CSVFile Generation is done Successfully" + "\n" + "Path:" + destinationFile);

		} catch (IOException | JSONException e) {
			logger.log(Level.SEVERE, "IOEXCEPTION/JSONEXCEPTION", e);
		}

	}

	public Address geoCodeAddress(String api, String benchmark, Address address)
			throws IOException, JSONException, ClassNotFoundException, SQLException{
		URL url = new URL(api + "street=" + URLEncoder.encode(address.getAddress1(), "UTF-8") + "&city="
				+ URLEncoder.encode(address.getCity(), "UTF-8") + "&state="
				+ URLEncoder.encode(address.getStateProvince(), "UTF-8") + "&zip="
				+ URLEncoder.encode(address.getZipCode(), "UTF-8"));
		URLConnection urlcon = url.openConnection();
		BufferedReader buff = new BufferedReader(new InputStreamReader(urlcon.getInputStream()));
		String geoCodeJSONResponseStr = "";
		while ((geoCodeJSONResponseStr = buff.readLine()) != null) {
			JSONObject object = new JSONObject(geoCodeJSONResponseStr);
			JSONObject resobj = object.getJSONObject("result");
			JSONArray addobj = resobj.getJSONArray("addressMatches");
			if (addobj.length() > 0) { // if there is at least one object in the addressMatches array.
				logger.info("URL" + url);
				JSONObject arrayobj = addobj.getJSONObject(0);
				JSONObject coor = arrayobj.getJSONObject("coordinates");
				String x = coor.getString("x");
				String y = coor.getString("y");
				logger.info("Longitude:" + x + "\tLatitude:" + y);
				address.setLongitude(x);
				address.setLatitude(y);
				String[] data = { address.getStloc_id(), address.getIdentifier(), address.getPhone(), address.getFax(),
						address.getAddress1(), address.getAddress2(), address.getAddress3(), address.getCity(),
						address.getStateProvince(), address.getCountry(), address.getZipCode(), address.getActive(),
						address.getLongitude(), address.getLatitude() };
				//Inserting data into the database by simply calling the Dbconnection Class.
				dbconn.insertion(data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7], data[8],
						data[9], data[10], data[11], data[12], data[13]);

				writerList.add(data);
			} else { // if there are no addressMatches then geocode them by using another api which
						// uses GeoCoderNames.
				address = geoCodeAddressNames(address);
			}
		}
		return address;
	}

	public Address geoCodeAddressNames(Address address)
			throws JSONException, IOException, ClassNotFoundException, SQLException {
		URL url1 = new URL("http://api.geonames.org/searchJSON?&maxRows=10&username=nidaan&"
				+ URLEncoder.encode(address.getCity(), "UTF-8") + "+"
				+ URLEncoder.encode(address.getStateProvince(), "UTF-8") + "+"
				+ URLEncoder.encode(address.getZipCode(), "UTF-8"));
		logger.info("" + url1);
		URLConnection urlcon1 = url1.openConnection();
		BufferedReader buff1 = new BufferedReader(new InputStreamReader(urlcon1.getInputStream()));
		String geoCodeJSONResponseStr1 = "";
		while ((geoCodeJSONResponseStr1 = buff1.readLine()) != null) {
			JSONObject object1 = new JSONObject(geoCodeJSONResponseStr1);
			JSONArray addobj1 = object1.getJSONArray("geonames");
			JSONObject arrayobj1 = addobj1.getJSONObject(0);
			String x = arrayobj1.getString("lng");
			String y = arrayobj1.getString("lat");
			logger.info("Longitude:" + x + "\tLatitude:" + y);
			address.setLongitude(x);
			address.setLatitude(y);
			String[] data = { address.getStloc_id(), address.getIdentifier(), address.getPhone(), address.getFax(),
					address.getAddress1(), address.getAddress2(), address.getAddress3(), address.getCity(),
					address.getStateProvince(), address.getCountry(), address.getZipCode(), address.getActive(),
					address.getLongitude(), address.getLatitude() };
			//Inserting data into the database by simply calling the Dbconnection Class.
			dbconn.insertion(data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7], data[8], data[9],
					data[10], data[11], data[12], data[13]);
			
			writerList.add(data);
		}
		return address;
	}

	public static void main(String[] args) throws Exception {

		// Command Line Argument to pass the api,benchmark,inputfile,outputfile and
		// separate them with spaces.
		GeoCoderExample geocoder = new GeoCoderExample();

		geocoder.setApi("https://geocoding.geo.census.gov/geocoder/locations/address?benchmark=9&format=json&");
		geocoder.geoCodeFile(args[0], args[1]);

	}
}
