package db_time.main;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;

public class RefreshThread extends Thread {
	private long totalTime;
	private long refreshTime;
	private PostgreSQL_Tester pgsql;

	/**
	 * 
	 * @param totalTime
	 * @param refreshTime
	 * @param pgsql
	 */
	RefreshThread(long totalTime, long refreshTime, PostgreSQL_Tester pgsql){
		this.totalTime = totalTime;
		this.refreshTime = refreshTime;
		this.pgsql = pgsql;
	}
	
	public void run(){
		
		long timeElapsed = 0;
		double milliElapsed = 0;
		long totalStart = System.nanoTime();
		long loops = 0;
		ArrayList<Long> data = new ArrayList<Long>();
		while (milliElapsed < totalTime) {
			long start = System.nanoTime();
			pgsql.refreshView("artist_knows");
			loops++;
			long end = System.nanoTime();
			timeElapsed += (end - start);
			data.add((end - start));
			pgsql.insertInfo("test");
			try {
				Thread.sleep(refreshTime);
			} catch (InterruptedException e) {
				System.out.println("Sleep failure");
				e.printStackTrace();
			}
			long totalEnd = System.nanoTime();
			milliElapsed = (totalEnd - totalStart) / 1000000;
		}
		System.out.println("Finish");
		pgsql.delete("test");
		double timeResult = ((timeElapsed / loops));
		double timeResult2 = timeResult / 1000000;
		DecimalFormat df = new DecimalFormat("00.00000");
		long max = Collections.max(data);
		long min = Collections.min(data);
		System.out.println("Max: " + max + " Min: " + min
				+ " Average in milli: " + df.format(timeResult2)
				+ " and total: " + timeElapsed);
	}
}
