package com.db.sql.demo;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import oracle.jdbc.OracleConnection;
import oracle.jdbc.pool.OracleDataSource;

public class DatabaseConnection {

	final static String DB_USER = "hr";
	final static String DB_PASSWORD = "hr";
	final static String DB_URL = "jdbc:oracle:thin:@localhost:1521/ORCLPDB";

	public static void main(String args[]) throws SQLException {
		Properties info = new Properties();
		info.put(OracleConnection.CONNECTION_PROPERTY_USER_NAME, DB_USER);
		info.put(OracleConnection.CONNECTION_PROPERTY_PASSWORD, DB_PASSWORD);
		info.put(OracleConnection.CONNECTION_PROPERTY_DEFAULT_ROW_PREFETCH, "20");

		OracleDataSource ods = new OracleDataSource();
		ods.setURL(DB_URL);
		ods.setConnectionProperties(info);

		// With AutoCloseable, the connection is closed automatically.
		try (OracleConnection connection = (OracleConnection) ods.getConnection()) {
			// Perform a database operation
			printAllEmployees(connection);
			printEmployeesWithSalary(connection);
		}
	}

	private static void printEmployeesWithSalary(OracleConnection connection) throws SQLException {
		try (Statement statement = connection.createStatement()) {
			try (ResultSet resultSet = statement.executeQuery("select first_name, salary from employees where salary > 10000")) {
				while (resultSet.next()) {
					System.out.println(resultSet.getString(1) + " " + resultSet.getString(2));
				}
			}
		}
	}

	public static void printAllEmployees(OracleConnection connection) throws SQLException {
		// Statement and ResultSet are AutoCloseable and closed automatically.
		try (Statement statement = connection.createStatement()) {
			try (ResultSet resultSet = statement.executeQuery("select * from employees")) {
				ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
				int columnsNumber = resultSetMetaData.getColumnCount();
				while (resultSet.next()) {
					for (int i = 1; i <= columnsNumber; i++) {
						String columnValue = resultSet.getString(i);
						System.out.print(columnValue + "\t");
					}
					System.out.println("");
				}
			}
		}
	}
}
