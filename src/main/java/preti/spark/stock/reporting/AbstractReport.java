package preti.spark.stock.reporting;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import preti.spark.stock.system.TradeSystem;

public abstract class AbstractReport implements Report {
	protected static final Log log = LogFactory.getLog(AbstractReport.class);

	protected TradeSystem system;
	protected String outputIp;
	protected int outputPort;
	protected String indexName;

	public AbstractReport(TradeSystem system, String outputIp, int outputPort, String indexName) {
		super();
		this.system = system;
		this.outputIp = outputIp;
		this.outputPort = outputPort;
		this.indexName = indexName;
	}

	@Override
	public void generate() {
		try {
			executeReport();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	protected abstract void executeReport() throws IOException;

}
