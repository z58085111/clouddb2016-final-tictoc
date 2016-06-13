package org.vanilladb.core.storage.tx;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.TSWord;
import org.vanilladb.core.sql.Tuple;
import org.vanilladb.core.sql.TupleType;
import org.vanilladb.core.storage.buffer.BufferMgr;
import org.vanilladb.core.storage.record.RecordFile;
import org.vanilladb.core.storage.record.RecordInfo;
import org.vanilladb.core.storage.tx.concurrency.ConcurrencyMgr;
import org.vanilladb.core.storage.tx.recovery.RecoveryMgr;

/**
 * Provides transaction management for clients, ensuring that all transactions
 * are recoverable, and in general satisfy the ACID properties with specified
 * isolation level.
 */
public class Transaction {
	private static Logger logger = Logger.getLogger(Transaction.class.getName());

	private RecoveryMgr recoveryMgr;
	private ConcurrencyMgr concurMgr;
	private BufferMgr bufferMgr;
	private List<TransactionLifecycleListener> lifecycleListeners;
	private long txNum;
	private boolean readOnly;

	private Map<RecordInfo, Tuple> readSet;
	private Map<RecordInfo, Tuple> writeSet;
	private Set<RecordInfo> insertRecs;
	private long commitTS;
	
	/**
	 * Creates a new transaction and associates it with a recovery manager and a
	 * concurrency manager. This constructor depends on the file, log, and
	 * buffer managers from {@link VanillaDb}, which are created during system
	 * initialization. Thus this constructor cannot be called until either
	 * {@link VanillaDb#init(String)} or
	 * {@link VanillaDb#initFileLogAndBufferMgr(String)} or is called first.
	 */
	public Transaction(TransactionMgr txMgr, TransactionLifecycleListener concurMgr,
			TransactionLifecycleListener recoveryMgr, TransactionLifecycleListener bufferMgr,
			boolean readOnly, long txNum) {
		this.concurMgr = (ConcurrencyMgr) concurMgr;
		this.recoveryMgr = (RecoveryMgr) recoveryMgr;
		this.bufferMgr = (BufferMgr) bufferMgr;
		this.txNum = txNum;
		this.readOnly = readOnly;
		this.readSet = new LinkedHashMap<RecordInfo, Tuple>();
		this.writeSet = new TreeMap<RecordInfo, Tuple>();
		this.insertRecs = new TreeSet<RecordInfo>();
		this.commitTS = 0;

		lifecycleListeners = new LinkedList<TransactionLifecycleListener>();
		// XXX: A transaction manager must be added before a recovery manager to
		// prevent the following scenario:
		// <COMMIT 1>
		// <NQCKPT 1,2>
		//
		// Although, it may create another scenario like this:
		// <NQCKPT 2> 
		// <COMMIT 1>
		// But the current algorithm can still recovery correctly during this scenario.
		addLifecycleListener(txMgr);
		/*
		 * A recover manager must be added before a concurrency manager.
		 * For example, if the transaction need to roll
		 * back, it must hold all locks until the recovery procedure complete.
		 */
		addLifecycleListener(recoveryMgr);
		addLifecycleListener(concurMgr);
		addLifecycleListener(bufferMgr);
	}

	public void addLifecycleListener(TransactionLifecycleListener listener) {
		lifecycleListeners.add(listener);
	}
	public Tuple getTuple(TupleType type, RecordInfo recInfo) {
		return (type == TupleType.READ)? readSet.get(recInfo) : writeSet.get(recInfo);
	}
	public void addTuple(Tuple t) {
		if(t.type() == TupleType.READ) {
			readSet.put(t.recordInfo(), t);
			if(t.TSWord().wts()>1)
			System.out.println("wts= "+t.TSWord().wts()+" ; commit_ts = "+this.commitTS);
		} else {
			writeSet.put(t.recordInfo(), t);
			if(t.type() == TupleType.INSERT)
				insertRecs.add(t.recordInfo());
		}
	}
	public long commitTS() {
		return this.commitTS;
	}
	/**
	 * Commits the current transaction. Flushes all modified blocks (and their
	 * log records), writes and flushes a commit record to the log, releases all
	 * locks, and unpins any pinned blocks.
	 */
	public void commit() {
		validate();
		write();
		for (TransactionLifecycleListener l : lifecycleListeners)
			l.onTxCommit(this);

		if (logger.isLoggable(Level.FINE))
			logger.fine("transaction " + txNum + " committed");
	}
	
	public void validate() throws InvalidException {
//		System.out.println("read size: "+readSet.size() + "\nwrite size: "+writeSet.size()+"\ninsert: "+insertRecs.size());
		// step 1: lock write set
		for(Tuple tuple : writeSet.values()) {
			RecordFile rf = tuple.openCurrentTuple(this, false);
			rf.recGetLock();
//			tuple.closeCurrentTuple();
		}
		
		// step 2: compute commit_ts
		this.commitTS = 0;
		for(Tuple tuple : writeSet.values()) {
			RecordFile rf = tuple.openCurrentTuple(this, false);
			TSWord tsw = rf.getTS_WORD();
			long curRTS = tsw.rts() + 1;
			if(curRTS > this.commitTS)
				this.commitTS = curRTS;
//			tuple.closeCurrentTuple();
		}
		for(Tuple tuple : readSet.values()) {
			long wts = tuple.TSWord().wts();
//			if(wts>1)
//				System.out.println("tsw= "+tuple.TSWord().tsw() +"; wts= "+tuple.TSWord().wts()+"; delta="+tuple.TSWord().delta());
			if(wts > this.commitTS){
				this.commitTS = wts;
//				System.out.println("wts= "+wts+" ; commit_ts = "+this.commitTS);
			}
		}

		// step 3: validate the read set
		for(Tuple tuple : readSet.values()) {
			RecordFile rf = tuple.openCurrentTuple(this, false);
			boolean success = true;
			TSWord tsw1, tsw2;
			TSWord readTSW = tuple.TSWord();
			do {
				success = true;
				tsw1 = tsw2 = rf.getTS_WORD();
				if( readTSW.wts()!=tsw1.wts() ) 
					throw new InvalidException("abort tx." + txNum + " because tuple is unclean. "+this.commitTS);
				if(	tsw1.rts()<=this.commitTS && rf.recIsLocked() && !writeSet.containsKey(tuple.recordInfo()) ) {
					System.out.println("v1.rts = "+tsw1.rts() + ";locked= " + rf.recIsLocked());
					throw new InvalidException("abort tx." + txNum + " because it is modifing by another txn: "+this.commitTS);
				}
				// extend the rts of tuple
				if(tsw1.rts()<=this.commitTS) {
					long delta = this.commitTS - tsw1.wts();
					long overflow = delta - 0x7fff;
					long shift = (overflow > 0)? (delta - overflow) : 0 ;
					long shiftedWTS = tsw2.wts()+shift;
					delta = delta - shift;
//					System.out.println("delta= "+delta+" ; shift= "+shift+" ; wts= "+shiftedWTS+" ; commit_ts = "+this.commitTS);
					if(tsw1.tsw()!=rf.getTS_WORD().tsw())
						success = false;
					else
						rf.setTS_WORD(delta, shiftedWTS);
				}
			} while (!success);
			tuple.closeCurrentTuple();
		}
	}
	
	private void write() {
		for(Tuple tuple : writeSet.values()) {
			tuple.executeUpdate(this);
		}
//		bufferMgr.flushAll(txNum);
	}
	/**
	 * Rolls back the current transaction. Undoes any modified values, flushes
	 * those blocks, writes and flushes a rollback record to the log, releases
	 * all locks, and unpins any pinned blocks.
	 */
	public void rollback() {
		rollbackInsert();
		for (TransactionLifecycleListener l : lifecycleListeners) {
			l.onTxRollback(this);
		}

		if (logger.isLoggable(Level.FINE))
			logger.fine("transaction " + txNum + " rolled back");
	}
	private void rollbackInsert() {
		for(RecordInfo recInfo : insertRecs) {
			Tuple tuple = writeSet.get(recInfo);
			RecordFile rf = tuple.openCurrentTuple(this, true);
			rf.delete();
			tuple.closeCurrentTuple();
		}
	}
	/**
	 * Finishes the current statement. Releases slocks obtained so far for
	 * repeatable read isolation level and does nothing in serializable
	 * isolation level. This method should be called after each SQL statement.
	 */
	public void endStatement() {
		for (TransactionLifecycleListener l : lifecycleListeners)
			l.onTxEndStatement(this);
	}

	public long getTransactionNumber() {
		return this.txNum;
	}

	public boolean isReadOnly() {
		return this.readOnly;
	}

	public RecoveryMgr recoveryMgr() {
		return recoveryMgr;
	}

	public ConcurrencyMgr concurrencyMgr() {
		return concurMgr;
	}

	public BufferMgr bufferMgr() {
		return bufferMgr;
	}
}
