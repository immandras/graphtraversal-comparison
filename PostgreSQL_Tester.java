package db_time.main;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.UUID;

public class PostgreSQL_Tester {
	Connection connection = null;

	public void connect(String url, String user, String password) {
		try {

			connection = DriverManager.getConnection(url, user, password);

			System.out.println("Connection Succesful!");
			connection.setSchema("musicbrainz");
		} catch (SQLException e) {
			System.out.println("Connection failure");
			e.printStackTrace();
		}
	}

	public void QueryDB(String sql, String column, int loops) {
		try {
			Statement stmt = connection.createStatement();

			long timeElapsed = 0;
			ArrayList<Long> data = new ArrayList<Long>();

			for (int i = 0; i < loops; i++) {

				long start = System.nanoTime();

				ResultSet rs = stmt.executeQuery(sql);

				HashSet<String> result = new HashSet<String>();
				while (rs.next()) {
					String name = rs.getString(column);
					result.add(name);
				}

				rs.close();

				long end = System.nanoTime();
				data.add((end - start));
				timeElapsed += (end - start);
			}

			double timeResult = ((timeElapsed / loops));
			double timeResult2 = timeResult / 1000000;
			DecimalFormat df = new DecimalFormat("00.00000");
			long max = Collections.max(data);
			long min = Collections.min(data);
			System.out.println("Max: " + max + " Min: " + min
					+ " Average in milli: " + df.format(timeResult2)
					+ " and total: " + timeElapsed);

		} catch (SQLException e) {
			System.out.println("Read failure");
			e.printStackTrace();
		}
	}

	public void insertInfo(String name) {

		try {
			Statement stmt = connection.createStatement();
			UUID uuid = UUID.randomUUID();
			String insert = "INSERT INTO artist (gid, name, sort_name) VALUES('"
					+ uuid + "', '" + name + "','testo')";

			stmt.executeUpdate(insert);
			// System.out.println("Insert Success");
		} catch (SQLException e) {
			if (e.getMessage().contains("duplicate")) {

			} else {
				System.out.println("Insert failure");
				e.printStackTrace();
			}
		}

	}

	public void delete(String name) {
		try {
			Statement stmt = connection.createStatement();

			String delete = "DELETE FROM artist WHERE name ='" + name + "'";
			stmt.executeUpdate(delete);
			System.out.println("SQL Delete Success");
		} catch (SQLException e) {
			System.out.println("Delete failure");
			e.printStackTrace();
		}
	}

	public void refreshViewTest(String view, int loops) {
		try {
			Statement stmt = connection.createStatement();
			String refresh = "REFRESH MATERIALIZED VIEW " + view;
			long timeElapsed = 0;
			ArrayList<Long> data = new ArrayList<Long>();

			for (int i = 0; i < loops; i++) {

				long start = System.nanoTime();
				stmt.executeUpdate(refresh);

				long end = System.nanoTime();
				data.add((end - start));
				timeElapsed += (end - start);
				System.out.println("Refresh "+i);
				this.insertInfo("test");
			}

			
			double timeResult = ((timeElapsed / loops));
			double timeResult2 = timeResult / 1000000;
			DecimalFormat df = new DecimalFormat("00.00000");
			long max = Collections.max(data);
			long min = Collections.min(data);
			System.out.println("Max: " + max + " Min: " + min
					+ " Average in milli: " + df.format(timeResult2)
					+ " and total: " + timeElapsed);
			this.delete("test");
		} catch (SQLException e) {
			System.out.println("Refresh failure");
			e.printStackTrace();
		}
//			catch (InterruptedException e) {
//			System.out.println("Sleep failure");
//			e.printStackTrace();
//		}
	}

	public void refreshView(String view) {
		try {
			Statement stmt = connection.createStatement();
			System.out.println("Refreshing View");
			String refresh = "REFRESH MATERIALIZED VIEW " + view;
			stmt.executeUpdate(refresh);
			System.out.println("Refresh Success");
		} catch (SQLException e) {
			System.out.println("Refresh failure");
			e.printStackTrace();
		}
	}

	public void timeTest(String sql, String column, long milliTime) {

		ArrayList<Long> data = new ArrayList<Long>();
		long timeElapsed = 0;
		double milliElapsed = 0;
		long loops = 0;
		long totalStart = System.nanoTime();
		while (milliElapsed < milliTime) {
			try {
				Statement stmt = connection.createStatement();

				long start = System.nanoTime();

				ResultSet rs = stmt.executeQuery(sql);

				HashSet<String> result = new HashSet<String>();
				while (rs.next()) {
					String name = rs.getString(column);
					result.add(name);
				}

				rs.close();

				long end = System.nanoTime();				
				loops++;
				data.add((end - start));
				timeElapsed += (end - start);
				if((end - start)< 60000000000L){
					long timeLeft = (30000000000L - (end - start))/1000000; 
					System.out.println("Time till next insert:"+ timeLeft);
					Thread.sleep(timeLeft);
				}
				long totalEnd = System.nanoTime();
				milliElapsed = (totalEnd - totalStart) / 1000000;

			} catch (SQLException e) {
				System.out.println("Read failure");
				e.printStackTrace();
			} catch (InterruptedException e) {
				System.out.println("Sleep failure");
				e.printStackTrace();
			}
		}
		System.out.println("Finished");
		double timeResult = ((timeElapsed / loops));
		double timeResult2 = timeResult / 1000000;
		DecimalFormat df = new DecimalFormat("00.00000");
		long max = Collections.max(data);
		long min = Collections.min(data);
		System.out.println("Max: " + max + " Min: " + min
				+ " Average in milli: " + df.format(timeResult2)
				+ " and total: " + milliElapsed + "ms - in nano: "
				+ timeElapsed);

	}

	public void close() {
		try {
			connection.close();

			System.out.println("Close success");
		} catch (SQLException e) {
			System.out.println("Close failure");
			e.printStackTrace();
		}
	}
}
