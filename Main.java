package db_time.main;

public class Main {

	public static void main(String[] args) {

		// passiveTest();

		// loadTest(0,3000);

//		 refreshTest();

		// refreshQuery();

		concurrentTest(600000, 120000);
	}

	public static void refreshQuery() {
		PostgreSQL_Tester pgsql = new PostgreSQL_Tester();
		pgsql.connect("jdbc:postgresql://localhost:5432/musicbrainz",
				"postgres", "admin");
		Neo4J_Tester neo4j = null;
		System.out.println("Query during Refresh Test ");
		InsertThread pgsqlLoader = new InsertThread("refresh", 0, 0, "test",
				neo4j, pgsql);
		String sqlViewAll = "SELECT p1.id, p1.name " + "FROM artist_knows p1 "
				+ "RIGHT JOIN " + "(SELECT p1.artistId AS id, p1.artistName "
				+ "FROM artist_to_label p1 "
				+ "WHERE p1.labelName = 'Special Rider Music' " + "INTERSECT "
				+ "SELECT p1.artistId, p1.artistName "
				+ "FROM artist_to_label p1 "
				+ "WHERE p1.labelName = 'CBS Records Inc.') AS T1 "
				+ "ON p1.entity1 = T1.id " + "GROUP BY p1.id, p1.name "
				+ "HAVING COUNT(*) = " + "(SELECT COUNT (T2.id) " + "FROM "
				+ "(SELECT p1.artistId AS id, p1.artistName "
				+ "FROM artist_to_label p1 "
				+ "WHERE p1.labelName = 'Special Rider Music' " + "INTERSECT "
				+ "SELECT p1.artistId, p1.artistName "
				+ "FROM artist_to_label p1 "
				+ "WHERE p1.labelName = 'CBS Records Inc.') AS T2)";
		pgsqlLoader.start();
		int loops = 100;
		System.out.println("ViewAll");
		pgsql.QueryDB(sqlViewAll, "name", loops);
		pgsqlLoader.endWork();
	}

	public static void concurrentTest(long totalTime, long refreshTime) {

		PostgreSQL_Tester pgsql = new PostgreSQL_Tester();

		System.out.println("Concurrent Test");
		pgsql.connect("jdbc:postgresql://localhost:5432/musicbrainz",
				"postgres", "admin");

		String sqlViewAll = "SELECT p1.id, p1.name " + "FROM artist_knows p1 "
				+ "RIGHT JOIN " + "(SELECT p1.artistId AS id, p1.artistName "
				+ "FROM artist_to_label p1 "
				+ "WHERE p1.labelName = 'Special Rider Music' " + "INTERSECT "
				+ "SELECT p1.artistId, p1.artistName "
				+ "FROM artist_to_label p1 "
				+ "WHERE p1.labelName = 'CBS Records Inc.') AS T1 "
				+ "ON p1.entity1 = T1.id " + "GROUP BY p1.id, p1.name "
				+ "HAVING COUNT(*) = " + "(SELECT COUNT (T2.id) " + "FROM "
				+ "(SELECT p1.artistId AS id, p1.artistName "
				+ "FROM artist_to_label p1 "
				+ "WHERE p1.labelName = 'Special Rider Music' " + "INTERSECT "
				+ "SELECT p1.artistId, p1.artistName "
				+ "FROM artist_to_label p1 "
				+ "WHERE p1.labelName = 'CBS Records Inc.') AS T2)";

		RefreshThread refreshThread = new RefreshThread(totalTime, refreshTime,
				pgsql);
		refreshThread.start();
		pgsql.timeTest(sqlViewAll, "name", 600000);
	}

	public static void refreshTest() {
		PostgreSQL_Tester pgsql = new PostgreSQL_Tester();

		System.out.println("Refresh Test");
		pgsql.connect("jdbc:postgresql://localhost:5432/musicbrainz",
				"postgres", "admin");

		pgsql.refreshViewTest("artist_knows", 10);

		pgsql.close();
	}

	public static void passiveTest() {
		PostgreSQL_Tester pgsql = new PostgreSQL_Tester();

		System.out.println("Passive Test");
		pgsql.connect("jdbc:postgresql://localhost:5432/musicbrainz",
				"postgres", "admin");
		Neo4J_Tester neo4j = new Neo4J_Tester("bolt://localhost:11001",
				"admin", "admin");
		String sqlView2 = " SELECT p1.id, p1.name FROM artist_knows p1 "
				+ "RIGHT JOIN (SELECT p1.id, p1.name FROM artist p1 "
				+ "LEFT JOIN l_artist_label pkp1 " + "ON p1.id = pkp1.entity0 "
				+ "LEFT JOIN label pl " + "ON pkp1.entity1 = pl.id "
				+ "WHERE pl.name = 'Special Rider Music' " + "INTERSECT "
				+ "SELECT p1.id, p1.name " + "FROM artist p1 "
				+ "LEFT JOIN l_artist_label pkp1 " + "ON p1.id = pkp1.entity0 "
				+ "LEFT JOIN label pl " + "ON pkp1.entity1 = pl.id "
				+ "WHERE pl.name = 'CBS Records Inc.') AS T1 "
				+ "ON p1.entity1 = T1.id GROUP BY p1.id, p1.name "
				+ "HAVING COUNT(*) = " + "(SELECT COUNT (T2.id) "
				+ "FROM (SELECT p1.id, p1.name " + "FROM artist p1 "
				+ "LEFT JOIN l_artist_label pkp1 " + "ON p1.id = pkp1.entity0 "
				+ "LEFT JOIN label pl " + "ON pkp1.entity1 = pl.id "
				+ "WHERE pl.name = 'Special Rider Music' " + "INTERSECT "
				+ "SELECT p1.id, p1.name " + "FROM artist p1 "
				+ "LEFT JOIN l_artist_label pkp1 " + "ON p1.id = pkp1.entity0 "
				+ "LEFT JOIN label pl " + "ON pkp1.entity1 = pl.id "
				+ "WHERE pl.name = 'CBS Records Inc.') AS T2)";

		String sqlViewAll = "SELECT p1.id, p1.name " + "FROM artist_knows p1 "
				+ "RIGHT JOIN " + "(SELECT p1.artistId AS id, p1.artistName "
				+ "FROM artist_to_label p1 "
				+ "WHERE p1.labelName = 'Special Rider Music' " + "INTERSECT "
				+ "SELECT p1.artistId, p1.artistName "
				+ "FROM artist_to_label p1 "
				+ "WHERE p1.labelName = 'CBS Records Inc.') AS T1 "
				+ "ON p1.entity1 = T1.id " + "GROUP BY p1.id, p1.name "
				+ "HAVING COUNT(*) = " + "(SELECT COUNT (T2.id) " + "FROM "
				+ "(SELECT p1.artistId AS id, p1.artistName "
				+ "FROM artist_to_label p1 "
				+ "WHERE p1.labelName = 'Special Rider Music' " + "INTERSECT "
				+ "SELECT p1.artistId, p1.artistName "
				+ "FROM artist_to_label p1 "
				+ "WHERE p1.labelName = 'CBS Records Inc.') AS T2)";

		String sqlWideTable = "SELECT a.artistid, a.artistname"
				+ " FROM wide_table2 a, wide_table2 b"
				+ " WHERE a.artistid2 = b.artistid"
				+ " AND b.labelname = 'CBS Records Inc.'" + " INTERSECT"
				+ " SELECT a.artistid, a.artistname"
				+ " FROM wide_table2 a, wide_table2 b"
				+ " WHERE a.artistid2 = b.artistid"
				+ " AND b.labelname = 'Special Rider Music'";

		int loops = 500;
		System.out.println("View2");
		pgsql.QueryDB(sqlView2, "name", loops);
		System.out.println("ViewAll");
		pgsql.QueryDB(sqlViewAll, "name", loops);
		System.out.println("WideTable");
		pgsql.QueryDB(sqlWideTable, "artistname", loops);

		System.out.println("Neo4J");
		neo4j.QueryDB(
				"MATCH (:label {name:'Special Rider Music'})<-[:connectedTo]-(b:artist)-[:connectedTo]->(:label {name:'CBS Records Inc.'})"
						+ " WITH collect(b) as persons"
						+ " UNWIND persons as b "
						+ " WITH size(persons) as total, b"
						+ " MATCH (a:artist)-[:knows]->(b)"
						+ " WITH total, a, count(a) as knownCount"
						+ " WHERE total = knownCount" + " RETURN a", loops);
		neo4j.close();
		pgsql.close();
	}

	public static void loadTest(long sleeptime, int inserts) {
		PostgreSQL_Tester pgsql = new PostgreSQL_Tester();
		Neo4J_Tester neo4j = new Neo4J_Tester("bolt://localhost:11001",
				"admin", "admin");
		pgsql.connect("jdbc:postgresql://localhost:5432/musicbrainz",
				"postgres", "admin");
		System.out.println("Load Test " + sleeptime);
		InsertThread neo4jLoader = new InsertThread("neo4j", inserts,
				sleeptime, "test", neo4j, pgsql);
		InsertThread pgsqlLoader = new InsertThread("pgsql", inserts,
				sleeptime, "test", neo4j, pgsql);

		// String sqlView2 =
		// " SELECT p1.id, p1.name FROM artist_knows p1 "
		// + "RIGHT JOIN (SELECT p1.id, p1.name FROM artist p1 "
		// + "LEFT JOIN l_artist_label pkp1 "
		// + "ON p1.id = pkp1.entity0 "
		// + "LEFT JOIN label pl "
		// + "ON pkp1.entity1 = pl.id "
		// + "WHERE pl.name = 'Special Rider Music' "
		// + "INTERSECT "
		// + "SELECT p1.id, p1.name "
		// + "FROM artist p1 "
		// + "LEFT JOIN l_artist_label pkp1 "
		// + "ON p1.id = pkp1.entity0 "
		// + "LEFT JOIN label pl "
		// + "ON pkp1.entity1 = pl.id "
		// + "WHERE pl.name = 'CBS Records Inc.') AS T1 "
		// + "ON p1.entity1 = T1.id GROUP BY p1.id, p1.name "
		// + "HAVING COUNT(*) = "
		// + "(SELECT COUNT (T2.id) "
		// + "FROM (SELECT p1.id, p1.name "
		// + "FROM artist p1 "
		// + "LEFT JOIN l_artist_label pkp1 "
		// + "ON p1.id = pkp1.entity0 "
		// + "LEFT JOIN label pl "
		// + "ON pkp1.entity1 = pl.id "
		// + "WHERE pl.name = 'Special Rider Music' "
		// + "INTERSECT "
		// + "SELECT p1.id, p1.name "
		// + "FROM artist p1 "
		// + "LEFT JOIN l_artist_label pkp1 "
		// + "ON p1.id = pkp1.entity0 "
		// + "LEFT JOIN label pl "
		// + "ON pkp1.entity1 = pl.id "
		// + "WHERE pl.name = 'CBS Records Inc.') AS T2)";

		String sqlViewAll = "SELECT p1.id, p1.name " + "FROM artist_knows p1 "
				+ "RIGHT JOIN " + "(SELECT p1.artistId AS id, p1.artistName "
				+ "FROM artist_to_label p1 "
				+ "WHERE p1.labelName = 'Special Rider Music' " + "INTERSECT "
				+ "SELECT p1.artistId, p1.artistName "
				+ "FROM artist_to_label p1 "
				+ "WHERE p1.labelName = 'CBS Records Inc.') AS T1 "
				+ "ON p1.entity1 = T1.id " + "GROUP BY p1.id, p1.name "
				+ "HAVING COUNT(*) = " + "(SELECT COUNT (T2.id) " + "FROM "
				+ "(SELECT p1.artistId AS id, p1.artistName "
				+ "FROM artist_to_label p1 "
				+ "WHERE p1.labelName = 'Special Rider Music' " + "INTERSECT "
				+ "SELECT p1.artistId, p1.artistName "
				+ "FROM artist_to_label p1 "
				+ "WHERE p1.labelName = 'CBS Records Inc.') AS T2)";

		String sqlWideTable = "SELECT a.artistid, a.artistname"
				+ " FROM wide_table2 a, wide_table2 b"
				+ " WHERE a.artistid2 = b.artistid"
				+ " AND b.labelname = 'CBS Records Inc.'" + " INTERSECT"
				+ " SELECT a.artistid, a.artistname"
				+ " FROM wide_table2 a, wide_table2 b"
				+ " WHERE a.artistid2 = b.artistid"
				+ " AND b.labelname = 'Special Rider Music'";

		pgsqlLoader.start();
		try {
			Thread.sleep(100);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		int loops = 500;
		// System.out.println("View2");
		// pgsql.QueryDB(sqlView2, "name", loops);
		System.out.println("ViewAll");
		pgsql.QueryDB(sqlViewAll, "name", loops);
		System.out.println("WideTable");
		pgsql.QueryDB(sqlWideTable, "artistname", loops);

		neo4jLoader.start();
		try {
			Thread.sleep(500);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("Neo4J");
		neo4j.QueryDB(
				"MATCH (:label {name:'Special Rider Music'})<-[:connectedTo]-(b:artist)-[:connectedTo]->(:label {name:'CBS Records Inc.'})"
						+ " WITH collect(b) as persons"
						+ " UNWIND persons as b "
						+ " WITH size(persons) as total, b"
						+ " MATCH (a:artist)-[:knows]->(b)"
						+ " WITH total, a, count(a) as knownCount"
						+ " WHERE total = knownCount" + " RETURN a", loops);
		// neo4j.close();
		// pgsql.close();
	}
}
