package com.query.application;

//STEP 1. Import required packages

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class QueryApplication {

	public static void main(String[] args) {

		Statement stmt = null;
		Connection conn = null;
		ResultSet rs = null;
		try {

			System.out.println("Getting database connection...");
			conn = getConnection();
			
			System.out.println("Creating statement...");
			stmt = conn.createStatement();
			
			String sql = getPOAssignmentsQuery();

		    rs = stmt.executeQuery(sql);

			List<UserPOAssignment> assignmentList = getUserPoAssignments(rs);

			printUserPOAssignments(assignmentList);

		} catch (SQLException se) {
			se.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {

			try {
				if (stmt != null)
					stmt.close();
				if(rs != null)
					rs.close();
			} catch (SQLException se2) {
			}
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException se) {
				se.printStackTrace();
			}
		}
	}

	/**
	 * Print User PO Assignments
	 * 
	 * @param assignmentList
	 */
	private static void printUserPOAssignments(List<UserPOAssignment> assignmentList) {

		if(assignmentList == null)
			return;
		
		for (UserPOAssignment assignment : assignmentList) {
			System.out.print(" userId: " + assignment.getUserId());
			System.out.print(" Po Number: " + assignment.getPoNumber());
			System.out.print(" Status: " + assignment.getStatus());
		}

	}

	/**
	 * Get Db Connection
	 * 
	 * @return
	 */
	private static Connection getConnection() {
		return DBConnection.getDBConnection();
	}

	/**
	 * Build select query
	 * 
	 * @return
	 */
	private static String getPOAssignmentsQuery() {
		StringBuilder sql = new StringBuilder();

		sql.append("SELECT user_id, po_num, status FROM User_PO_Assignment");

		return sql.toString();

	}

	/**
	 * Fetch User PO Assignments
	 * 
	 * @param rs
	 * @return
	 * @throws SQLException
	 */
	private static List<UserPOAssignment> getUserPoAssignments(ResultSet rs) throws SQLException {

		List<UserPOAssignment> assignments = new ArrayList<UserPOAssignment>();
		UserPOAssignment assignment;
		while (rs.next()) {
			assignment = new UserPOAssignment();
			assignment.setUserId(rs.getInt("user_id"));
			assignment.setPoNumber(rs.getInt("po_num"));
			assignment.setStatus(rs.getString("status"));

			assignments.add(assignment);
		}

		return assignments;

	}

}
