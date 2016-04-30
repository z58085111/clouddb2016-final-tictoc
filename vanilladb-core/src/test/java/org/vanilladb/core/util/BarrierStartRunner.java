package org.vanilladb.core.util;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

public abstract class BarrierStartRunner extends Thread {

	private CyclicBarrier startBarrier;
	private CyclicBarrier endBarrier;

	private Exception exception;

	public BarrierStartRunner(CyclicBarrier startBarrier, CyclicBarrier endBarrier) {
		this.startBarrier = startBarrier;
		this.endBarrier = endBarrier;
	}

	public abstract void runTask();

	public void beforeTask() {

	}

	public void afterTask() {

	}

	public Exception getException() {
		return exception;
	}

	@Override
	public void run() {
		try {
			beforeTask();
		} catch (Exception e) {
			exception = e;
		}
		
		try {
			startBarrier.await();
		} catch (InterruptedException | BrokenBarrierException e1) {
			e1.printStackTrace();
		}
		
		try {
			runTask();
		} catch (Exception e) {
			exception = e;
		}
		
		try {
			endBarrier.await();
		} catch (InterruptedException | BrokenBarrierException e) {
			e.printStackTrace();
		}
		
		try {
			afterTask();
		} catch (Exception e) {
			exception = e;
		}
	}
}
