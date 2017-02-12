package org.vanilladb.core.storage.buffer;

import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.Type;

/**
 * An interface used to initialize a new block on disk. There will be an
 * implementing class for each "type" of disk block.
 */
public abstract class PageFormatter {
	/**
	 * Initializes a page, whose contents will be written to a new disk block.
	 * This method is called only during the method {@link Buffer#assignToNew}.
	 * 
	 * @param buf
	 *            a buffered page
	 */
	public abstract void format(Buffer buf);
	
	protected void setVal(Buffer buf, int offset, Constant val) {
		buf.setVal(offset, val);
	}
	
	protected Constant getVal(Buffer buf, int offset, Type type) {
		return buf.getVal(offset, type);
	}
}
