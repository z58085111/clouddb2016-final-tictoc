package org.vanilladb.core.storage.buffer;

import java.io.IOException;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

import org.junit.BeforeClass;
import org.junit.Test;
import org.vanilladb.core.server.ServerInit;
import org.vanilladb.core.util.BarrierStartRunner;

import junit.framework.Assert;

public class BufferConcurrencyTest {

	private static final int CLIENT_COUNT = 100;

	@BeforeClass
	public static void init() throws IOException {
		ServerInit.init();
	}

	@Test
	public void testConcourrentPinning() {
		Buffer buffer = new Buffer();
		CyclicBarrier startBarrier = new CyclicBarrier(CLIENT_COUNT);
		CyclicBarrier endBarrier = new CyclicBarrier(CLIENT_COUNT + 1);

		// Create multiple threads
		for (int i = 0; i < CLIENT_COUNT; i++)
			new Pinner(startBarrier, endBarrier, buffer).start();

		// Wait for running
		try {
			endBarrier.await();
		} catch (InterruptedException | BrokenBarrierException e) {
			e.printStackTrace();
		}

		// Check the results
		Assert.assertEquals("testBufferPinCount failed", buffer.isPinned(), false);
	}

	class Pinner extends BarrierStartRunner {

		Buffer buf;

		public Pinner(CyclicBarrier startBarrier, CyclicBarrier endBarrier, Buffer buf) {
			super(startBarrier, endBarrier);

			this.buf = buf;
		}

		@Override
		public void runTask() {
			for (int i = 0; i < 10000; i++) {
				buf.pin();
				buf.unpin();
			}
		}

	}
}
