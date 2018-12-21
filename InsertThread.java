package db_time.main;

public class InsertThread extends Thread {
	private String view;
	private int change;
	private String insert;
	private PostgreSQL_Tester pgsql;
	private Neo4J_Tester neo4j;
	private long sleeptime;
	private boolean stopped = false;

	InsertThread(String view, int change, long sleeptime, String insert,
			Neo4J_Tester neo4j, PostgreSQL_Tester pgsql) {
		this.view = view;
		this.change = change;
		this.insert = insert;
		this.sleeptime = sleeptime;
		this.neo4j = neo4j;
		this.pgsql = pgsql;
	}

	public void run() {
		int count = 0;
		if (view.equals("neo4j")) {
			long timeElapsed = 0;
			long start = System.nanoTime();
			while (!this.isInterrupted() & count < change) {

				neo4j.insertInfo(insert);
				count++;

				try {
					Thread.sleep(sleeptime);
				} catch (InterruptedException e) {
					System.out.println("Sleep Interrupted");
					e.printStackTrace();
				}
			}
			long end = System.nanoTime();
			timeElapsed = (end - start);
			System.out.println("Neo4j Insert finished in " + timeElapsed);
			neo4j.delete(insert);
			neo4j.close();

		} else if (view.equals("refresh")) {
			
			while(!stopped){
				pgsql.refreshView("artist_knows");
			}
			pgsql.close();
		} else {
			long timeElapsed = 0;
			long start = System.nanoTime();
			while (!this.isInterrupted() & count < change) {

				pgsql.insertInfo(insert);
				count++;

				try {
					Thread.sleep(sleeptime);
				} catch (InterruptedException e) {
					System.out.println("Sleep Interrupted");
					e.printStackTrace();
				}
			}
			// pgsql.refreshView("artist_knows");
			long end = System.nanoTime();
			timeElapsed = (end - start);
			System.out.println("SQL Insert finished in " + timeElapsed);
			pgsql.delete(insert);
			pgsql.close();

		}
	}

	public void endWork() {
		stopped = true;
	}
}
