package netdb.software.benchmark.util;

import org.vanilladb.core.util.PropertiesLoader;

public class BenchProperties extends PropertiesLoader {
	
	private static BenchProperties loader;

	public static BenchProperties getLoader() {
		// Singleton
		if (loader == null)
			loader = new BenchProperties();
		return loader;
	}

	protected BenchProperties() {
		super();
	}

	@Override
	protected String getConfigFilePath() {
		return System.getProperty("netdb.software.benchmark.config.file");
	}

}
