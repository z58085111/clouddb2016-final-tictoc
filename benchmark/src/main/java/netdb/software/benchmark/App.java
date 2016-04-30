package netdb.software.benchmark;

import java.util.logging.Level;
import java.util.logging.Logger;

import netdb.software.benchmark.util.BenchProperties;

public class App {

	private static Logger logger = Logger.getLogger(App.class.getName());
	static boolean LOAD_TESTBED, RUN_BENCHMARK;

	static {
		LOAD_TESTBED = BenchProperties.getLoader().getPropertyAsBoolean(
				App.class.getName() + ".LOAD_TESTBED", false);
		RUN_BENCHMARK = BenchProperties.getLoader().getPropertyAsBoolean(
				App.class.getName() + ".RUN_BENCHMARK", true);
	}

	public static void main(String[] args) {
		Benchmarker b = new Benchmarker();

		if (LOAD_TESTBED)
			b.load();

		// b.fullTableScan();

		if (RUN_BENCHMARK) {
			b.run();

			if (logger.isLoggable(Level.INFO))
				logger.info("benchmark period end...");
		}
		b.report();
	}
}
