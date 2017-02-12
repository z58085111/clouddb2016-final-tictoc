package org.vanilladb.core.storage.file.io;

import java.io.IOException;

public interface IoChannel {
	
	int read(IoBuffer buffer, long position) throws IOException;
	
	int write(IoBuffer buffer, long position) throws IOException;
	
	long append(IoBuffer buffer) throws IOException;
	
	long size() throws IOException;
	
	void close() throws IOException;
}
