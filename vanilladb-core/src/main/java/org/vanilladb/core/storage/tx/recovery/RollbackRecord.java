package org.vanilladb.core.storage.tx.recovery;

import static org.vanilladb.core.sql.Type.BIGINT;

import java.util.LinkedList;
import java.util.List;

import org.vanilladb.core.sql.BigIntConstant;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.storage.log.BasicLogRecord;
import org.vanilladb.core.storage.log.LogSeqNum;
import org.vanilladb.core.storage.tx.Transaction;

/**
 * The rollback log record.
 */
class RollbackRecord implements LogRecord {
	private long txNum;
	private LogSeqNum lsn;

	/**
	 * Creates a new rollback log record for the specified transaction.
	 * 
	 * @param txNum
	 *            the ID of the specified transaction
	 */
	public RollbackRecord(long txNum) {
		this.txNum = txNum;
		this.lsn = null;
	}

	/**
	 * Creates a log record by reading one other value from the log.
	 * 
	 * @param rec
	 *            the basic log record
	 */
	public RollbackRecord(BasicLogRecord rec) {
		this.txNum = (Long) rec.nextVal(BIGINT).asJavaVal();
		lsn = rec.getLSN();
	}

	/**
	 * Writes a rollback record to the log. This log record contains the
	 * {@link LogRecord#OP_ROLLBACK} operator ID, followed by the transaction
	 * ID.
	 * 
	 * @return the LSN of the log record
	 */
	@Override
	public LogSeqNum writeToLog() {
		List<Constant> rec = buildRecord();
		return logMgr.append(rec.toArray(new Constant[rec.size()]));
	}

	@Override
	public int op() {
		return OP_ROLLBACK;
	}

	@Override
	public long txNumber() {
		return txNum;
	}

	/**
	 * Does nothing, because a rollback record contains no undo information.
	 */
	@Override
	public void undo(Transaction tx) {
		// do nothing
	}

	/**
	 * Does nothing, because a rollback record contains no redo information.
	 */
	@Override
	public void redo(Transaction tx) {
		// do nothing
	}

	@Override
	public String toString() {
		return "<ROLLBACK " + txNum + ">";
	}

	@Override
	public List<Constant> buildRecord() {
		List<Constant> rec = new LinkedList<Constant>();
		rec.add(new IntegerConstant(op()));
		rec.add(new BigIntConstant(txNum));
		return rec;
	}

	@Override
	public LogSeqNum getLSN() {

		return lsn;
	}
}
