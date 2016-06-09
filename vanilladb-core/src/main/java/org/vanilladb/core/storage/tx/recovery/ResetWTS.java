package org.vanilladb.core.storage.tx.recovery;

import java.io.IOException;
import java.sql.Connection;

import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.server.task.Task;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.util.CoreProperties;

public class ResetWTS extends Task{
	
	private static final long PERIOD;
	private static long timeThreshold;
	private long accumulatedTime;
	private String tablename;
	
	static {
		PERIOD = CoreProperties.getLoader().getPropertyAsLong(
				CheckpointTask.class.getName() + ".PERIOD", 30000);
	}
	
	public ResetWTS(long accumulatedTime, String tablename) {
		this.accumulatedTime = accumulatedTime;
		this.tablename = tablename;
		timeThreshold = 5*PERIOD;
	}
	
	public static long readTimeFile(String tblname) throws IOException {
		return VanillaDb.fileMgr().readTimeFile(tblname);
	}
	
	public void writeTimeFile() {
		System.out.println("write file: "+tablename);
		VanillaDb.fileMgr().rebuildTimeFile(accumulatedTime, tablename);
	}
	
	public void updateTime() {
		if(accumulatedTime >= timeThreshold) {
			Transaction tx = VanillaDb.txMgr().newTransaction(Connection.TRANSACTION_SERIALIZABLE, false);
			VanillaDb.txMgr().resetWTS(tx, tablename);
			tx.commit();
			accumulatedTime = 0;
		} else {
			accumulatedTime+=PERIOD;
			writeTimeFile();
		}
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		while(true) {
			updateTime();
			try {
				Thread.sleep(PERIOD);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

}