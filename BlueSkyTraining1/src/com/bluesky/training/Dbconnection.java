package com.bluesky.training;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


public class Dbconnection {

	public void insertion(String STLOC_ID, String IDENTIFIER, String PHONE, String FAX, String ADDRESS1,
			String ADDRESS2, String ADDRESS3, String CITY, String STATE, String COUNTRY, String ZIPCODE, String ACTIVE,
			String Longitude, String Latitude) throws ClassNotFoundException, SQLException {
		Connection conn = null;
		PreparedStatement pstmt = null;
		Statement stmt = null;
		Class.forName("org.h2.Driver");
		conn = DriverManager.getConnection("jdbc:h2:~/test2", "sa", "");
		stmt = conn.createStatement();
		stmt.execute(
				"CREATE TABLE IF NOT EXISTS STOREDDATASAMPLE(STLOC_ID VARCHAR(255), IDENTIFIER VARCHAR(255), PHONE VARCHAR(255), FAX VARCHAR(255),ADDRESS1 VARCHAR(255), ADDRESS2 VARCHAR(255), ADDRESS3 VARCHAR(255),CITY VARCHAR(255), STATE VARCHAR(255), COUNTRY VARCHAR(255),ZIPCODE VARCHAR(255),ACTIVE VARCHAR(255), Longitude VARCHAR(255), Latitude VARCHAR(255),PRIMARY KEY(STLOC_ID)) ");
		pstmt = conn.prepareStatement("SELECT STLOC_ID FROM  STOREDDATASAMPLE where STLOC_ID =?");
		pstmt.setString(1, STLOC_ID);
		ResultSet rs = pstmt.executeQuery();
		if (!rs.next()) {//STLOC_ID is not present in the table then insert it in the table..
			pstmt = conn.prepareStatement(
					"insert into STOREDDATASAMPLE(STLOC_ID,IDENTIFIER,PHONE,FAX,ADDRESS1,ADDRESS2,ADDRESS3,CITY,STATE,COUNTRY,ZIPCODE,ACTIVE,Longitude,Latitude) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
			pstmt.setString(1, STLOC_ID);
			pstmt.setString(2, IDENTIFIER);
			pstmt.setString(3, PHONE);
			pstmt.setString(4, FAX);
			pstmt.setString(5, ADDRESS1);
			pstmt.setString(6, ADDRESS2);
			pstmt.setString(7, ADDRESS3);
			pstmt.setString(8, CITY);
			pstmt.setString(9, STATE);
			pstmt.setString(10, COUNTRY);
			pstmt.setString(11, ZIPCODE);
			pstmt.setString(12, ACTIVE);
			pstmt.setString(13, Longitude);
			pstmt.setString(14, Latitude);

			pstmt.executeUpdate();
			rs.close();
		} else {//if STLOC_ID is present in then table then remove  row in the table and then perform insertion..
			
			stmt.execute("truncate table STOREDDATASAMPLE");
			insertion(STLOC_ID,IDENTIFIER,PHONE,FAX,ADDRESS1,ADDRESS2,ADDRESS3,CITY,STATE,COUNTRY,ZIPCODE,ACTIVE,Longitude,Latitude);

		}
		pstmt.close();
		conn.close();

	}
}
