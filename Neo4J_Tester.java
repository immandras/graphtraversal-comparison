package db_time.main;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;

import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;

public class Neo4J_Tester {
	
	private final Driver driver;
	
	public Neo4J_Tester(String uri, String user, String password){
		driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password));
	}

	public void QueryDB(String query, int loops){
		Session session = driver.session();
		
		long timeElapsed = 0;
		ArrayList<Long> data = new ArrayList<Long>();
		
		for (int i=0;i<loops;i++){	
			
		long start = System.nanoTime();
		HashSet<Record> resultSet = new HashSet<Record>();
		StatementResult result = session.run(query);
		while (result.hasNext())
        {
            Record record = result.next();
            resultSet.add(record);

        }
		long end = System.nanoTime();
		data.add((end - start));
		timeElapsed += (end - start);
		}
		
		double timeResult = ((timeElapsed/loops));
		double timeResult2 = timeResult/1000000;
		DecimalFormat df = new DecimalFormat("00.00000");
		long max = Collections.max(data);
		long min = Collections.min(data);
		System.out.println("Max: "+max+" Min: "+min
				+" Average in milli: "+df.format(timeResult2)+" and total: "+timeElapsed);
	}
	
	public void insertInfo(String name){
		Session session = driver.session();
		String query = "CREATE (n:Person {name: '"+name+"'})";
		
		session.run(query);
	}
	
	public void delete(String name){
		Session session = driver.session();
		String query = "MATCH (n:Person {name: '"+name+"'}) DELETE n";
		
		session.run(query);
		
		System.out.println("Neo4j Delete Success");
	}
	
	public void close(){
		driver.close();
	}
}
