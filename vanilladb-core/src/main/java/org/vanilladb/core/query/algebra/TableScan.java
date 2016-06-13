package org.vanilladb.core.query.algebra;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.TSWord;
import org.vanilladb.core.sql.Tuple;
import org.vanilladb.core.sql.TupleType;
import org.vanilladb.core.storage.metadata.TableInfo;
import org.vanilladb.core.storage.record.RecordFile;
import org.vanilladb.core.storage.record.RecordId;
import org.vanilladb.core.storage.record.RecordInfo;
import org.vanilladb.core.storage.tx.Transaction;

/**
 * The Scan class corresponding to a table. A table scan is just a wrapper for a
 * RecordFile object; most methods just delegate to the corresponding RecordFile
 * methods.
 */
public class TableScan implements UpdateScan {
	private RecordFile rf;
	private Schema schema;
	private Transaction tx;
	private TableInfo ti;
	/**
	 * Creates a new table scan, and opens its corresponding record file.
	 * 
	 * @param ti
	 *            the table's metadata
	 * @param tx
	 *            the calling transaction
	 */
	public TableScan(TableInfo ti, Transaction tx) {
		this.tx = tx;
		this.ti = ti;
		rf = ti.open(tx, true);
		schema = ti.schema();
	}

	// Scan methods

	@Override
	public void beforeFirst() {
		rf.beforeFirst();
	}

	@Override
	public boolean next() {
		return rf.next();
	}

	@Override
	public void close() {
		rf.close();
	}

	/**
	 * Returns the value of the specified field, as a Constant.
	 * 
	 * @see Scan#getVal(java.lang.String)
	 */
	@Override
	public Constant getVal(String fldName) {
		Tuple t = addTxnTuple(TupleType.READ);
		return t.getVal(fldName);
	}

	@Override
	public boolean hasField(String fldName) {
		return schema.hasField(fldName);
	}

	// UpdateScan methods

	/**
	 * Sets the value of the specified field, as a Constant.
	 * 
	 * @param val
	 *            the constant to be set. Will be casted to the correct type
	 *            specified in the schema of the table.
	 * 
	 * @see UpdateScan#setVal(java.lang.String, Constant)
	 */
	@Override
	public void setVal(String fldName, Constant val) {
		Tuple t = addTxnTuple(TupleType.MODIFY);
		t.setVal(fldName, val);
//		rf.setVal(fldName, val);
	}

	@Override
	public void delete() {
		addTxnTuple(TupleType.DELETE);
//		rf.delete();
	}

	@Override
	public void insert() {
		rf.insert();
		addTxnTuple(TupleType.INSERT);
	}

	@Override
	public RecordId getRecordId() {
		return rf.currentRecordId();
	}

	@Override
	public void moveToRecordId(RecordId rid) {
		rf.moveToRecordId(rid);
	}
	
	private Tuple addTxnTuple(TupleType type) {
		RecordInfo recInfo = new RecordInfo(ti, getRecordId());
		Tuple t = tx.getTuple(type, recInfo);
		if(t == null) {
			t = atomicallyLoadTuple(type, recInfo);
			tx.addTuple(t);
		}
		return t;
	}
	
	private Tuple atomicallyLoadTuple(TupleType type, RecordInfo recInfo) {
		TSWord v1, v2;
		Map<String, Constant> recVal;
		RecordFile rf = recInfo.open(tx, false);
		do {
			v1 = rf.getTS_WORD();
			recVal = new LinkedHashMap<String, Constant>();
			Set<String> fields = recInfo.tableInfo().schema().fields();
			for(String fld : fields) {
				recVal.put(fld, rf.getVal(fld));
			}
			v2 = rf.getTS_WORD();
//			System.out.println("v1: "+v1.tsw()+" v2: "+v2.tsw()+" equal: "+v1.equals(v2)+" lock: "+rf.recIsLocked());
		} while ( !v1.equals(v2) || rf.recIsLocked() );
		recInfo.close();
		return new Tuple(type, recInfo, v1, recVal);
	}
}
