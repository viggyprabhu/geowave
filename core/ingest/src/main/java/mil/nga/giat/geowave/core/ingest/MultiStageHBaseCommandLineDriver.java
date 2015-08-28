package mil.nga.giat.geowave.core.ingest;

import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/**
 * @author viggy Functionality similar to
 *         <code> MultiStageCommandLineDriver </code>
 */
public class MultiStageHBaseCommandLineDriver extends
		AbstractIngestCommandLineDriver
{
	private final AbstractIngestCommandLineDriver[] orderedStages;

	public MultiStageHBaseCommandLineDriver(
			final String operation,
			final AbstractIngestCommandLineDriver[] orderedStages ) {
		super(
				operation);
		this.orderedStages = orderedStages;
	}

	@Override
	protected void runInternal(
			final String[] args,
			final List<IngestFormatPluginProviderSpi<?, ?>> pluginProviders ) {
		for (final AbstractIngestCommandLineDriver stage : orderedStages) {
			stage.runInternal(
					args,
					pluginProviders);
		}
	}

	@Override
	public void parseOptionsInternal(
			final CommandLine commandLine )
			throws ParseException {
		for (final AbstractIngestCommandLineDriver stage : orderedStages) {
			stage.parseOptionsInternal(commandLine);
		}
	}

	@Override
	public void applyOptionsInternal(
			final Options allOptions ) {
		for (final AbstractIngestCommandLineDriver stage : orderedStages) {
			stage.applyOptionsInternal(allOptions);
		}
	}

}
