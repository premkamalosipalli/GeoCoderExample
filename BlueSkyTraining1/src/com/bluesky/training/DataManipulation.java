package com.bluesky.training;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.LinkedHashMap;

public class DataManipulation {
	Connection conn = null;
	PreparedStatement pstmt = null;
	Statement stmt, stmt2;
	ResultSet resultset, resultset1;

	public void tableComparision(String sourcefile, String deltafile) throws ClassNotFoundException, SQLException {

		Class.forName("org.h2.Driver");
		conn = DriverManager.getConnection("jdbc:h2:~/test2", "sa", "");
		stmt = conn.createStatement();
		stmt2 = conn.createStatement();

		stmt.execute(
				"CREATE TABLE IF NOT EXISTS SOURCE(STLOC_ID INT PRIMARY KEY, IDENTIFIER VARCHAR(255), PHONE VARCHAR(255),"
						+ " FAX VARCHAR(255),ADDRESS1 VARCHAR(255), ADDRESS2 VARCHAR(255), ADDRESS3 VARCHAR(255),CITY VARCHAR(255),"
						+ " STATE VARCHAR(255), COUNTRY VARCHAR(255),ZIPCODE VARCHAR(255),ACTIVE VARCHAR(255)) "
						+ "AS SELECT * FROM CSVREAD('" + sourcefile + "')");
		stmt.execute(
				"CREATE TABLE IF NOT EXISTS DELTA(STLOC_ID INT PRIMARY KEY, IDENTIFIER VARCHAR(255), PHONE VARCHAR(255), "
						+ "FAX VARCHAR(255),ADDRESS1 VARCHAR(255), ADDRESS2 VARCHAR(255), ADDRESS3 VARCHAR(255),CITY VARCHAR(255), "
						+ "STATE VARCHAR(255), COUNTRY VARCHAR(255),ZIPCODE VARCHAR(255),ACTIVE VARCHAR(255))"
						+ " AS SELECT * FROM CSVREAD('" + deltafile + "')");

		resultset = stmt.executeQuery("select * from SOURCE");
		resultset1 = stmt2.executeQuery("select * from DELTA");

		HashMap<Integer, String> sourcemap = new LinkedHashMap<Integer, String>();
		while (resultset.next()) {
			String temp = toString(resultset);
			sourcemap.put(resultset.getInt("STLOC_ID"), temp);
		}

		HashMap<Integer, String> deltamap = new LinkedHashMap<Integer, String>();
		while (resultset1.next()) {
			String temp = toString(resultset1);
			deltamap.put(resultset1.getInt("STLOC_ID"), temp);

		}

		for (Object key : sourcemap.keySet()) {
			if (deltamap.get(key) != null) {

				ResultSet resultset11 = stmt2.executeQuery("select * from DELTA WHERE STLOC_ID=" + key + ";");
				while (resultset11.next()) {
					String sql = "UPDATE DELTA set STLOC_ID=?,IDENTIFIER=?,PHONE=?,FAX=?,ADDRESS1=?,"
							+ "ADDRESS2 =?,ADDRESS3=?,CITY=?,STATE=?,COUNTRY=?,ZIPCODE=?,ACTIVE=? WHERE STLOC_ID=" + key
							+ ";";
					dataModification(resultset11, sql);
				}
			} else {
				ResultSet resultset12 = stmt2.executeQuery("select * from SOURCE WHERE STLOC_ID=" + key + ";");
				while (resultset12.next()) {
					String sql = "insert into DELTA(STLOC_ID,IDENTIFIER,PHONE,FAX,ADDRESS1,ADDRESS2,ADDRESS3,CITY,STATE,COUNTRY,ZIPCODE,ACTIVE) values(?,?,?,?,?,?,?,?,?,?,?,?)";
					dataModification(resultset12, sql);
				}
			}
		}
		stmt.close();
		resultset.close();
		stmt2.close();
		resultset1.close();
		conn.close();

	}

	public String toString(ResultSet resultset) throws SQLException {
		String temp = resultset.getString("IDENTIFIER") + "," + resultset.getString("PHONE") + ","
				+ resultset.getString("FAX") + "," + resultset.getString("ADDRESS1") + ","
				+ resultset.getString("ADDRESS2") + "," + resultset.getString("ADDRESS3") + ","
				+ resultset.getString("CITY") + "," + resultset.getString("STATE") + ","
				+ resultset.getString("COUNTRY") + "," + resultset.getString("ZIPCODE") + ","
				+ resultset.getString("ACTIVE");

		return temp;
	}

	public void dataModification(ResultSet resultset, String query) throws SQLException {
		pstmt = conn.prepareStatement(query);
		pstmt.setInt(1, resultset.getInt("STLOC_ID"));
		pstmt.setString(2, resultset.getString("IDENTIFIER"));
		pstmt.setString(3, resultset.getString("PHONE"));
		pstmt.setString(4, resultset.getString("FAX"));
		pstmt.setString(5, resultset.getString("ADDRESS1"));
		pstmt.setString(6, resultset.getString("ADDRESS2"));
		pstmt.setString(7, resultset.getString("ADDRESS3"));
		pstmt.setString(8, resultset.getString("CITY"));
		pstmt.setString(9, resultset.getString("STATE"));
		pstmt.setString(10, resultset.getString("COUNTRY"));
		pstmt.setString(11, resultset.getString("ZIPCODE"));
		pstmt.setString(12, resultset.getString("ACTIVE"));
		pstmt.executeUpdate();
	}

	public static void main(String[] args) throws ClassNotFoundException, SQLException {

		DataManipulation manipulate = new DataManipulation();

		manipulate.tableComparision("/Users/bluesky/Downloads/source.csv", "/Users/bluesky/Downloads/delta.csv");
		System.out.println("-----------Execution Completed---------------");
	}

}
