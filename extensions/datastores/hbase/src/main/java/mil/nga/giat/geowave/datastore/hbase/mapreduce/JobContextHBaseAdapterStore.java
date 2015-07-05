/**
 * 
 */
package mil.nga.giat.geowave.datastore.hbase.mapreduce;

import java.util.HashMap;
import java.util.Map;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.datastore.hbase.operations.BasicHBaseOperations;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.log4j.Logger;

/**
 * @author viggy
 * 
 */
public class JobContextHBaseAdapterStore implements
		AdapterStore
{

	private static final Class<?> CLASS = JobContextHBaseAdapterStore.class;
	private final JobContext context;
	private final BasicHBaseOperations operations;
	private final Map<ByteArrayId, DataAdapter<?>> adapterCache = new HashMap<ByteArrayId, DataAdapter<?>>();
	protected static final Logger LOGGER = Logger.getLogger(CLASS);

	public JobContextHBaseAdapterStore(
			final JobContext context,
			final BasicHBaseOperations operations ) {
		this.context = context;
		this.operations = operations;

	}

	@Override
	public void addAdapter(
			DataAdapter<?> adapter ) {
		// TODO #406 Need to fix
		LOGGER.warn("Need to code this method addAdapter1");
	}

	@Override
	public DataAdapter<?> getAdapter(
			ByteArrayId adapterId ) {
		// TODO #406 Need to fix
		LOGGER.warn("Need to code this method getAdapter1");
		return null;
	}

	@Override
	public boolean adapterExists(
			ByteArrayId adapterId ) {
		// TODO #406 Need to fix
		LOGGER.warn("Need to code this method adapterExists1");
		return false;
	}

	@Override
	public CloseableIterator<DataAdapter<?>> getAdapters() {
		// TODO #406 Need to fix
		LOGGER.warn("Need to code this method getAdapters");
		return null;
	}

	public static DataAdapter<?>[] getDataAdapters(
			final JobContext context ) {
		return GeoWaveHBaseConfiguratorBase.getDataAdapters(
				CLASS,
				context);
	}

	public static void addDataAdapter(
			final Configuration configuration,
			final DataAdapter<?> adapter ) {
		GeoWaveHBaseConfiguratorBase.addDataAdapter(
				CLASS,
				configuration,
				adapter);
	}
}
