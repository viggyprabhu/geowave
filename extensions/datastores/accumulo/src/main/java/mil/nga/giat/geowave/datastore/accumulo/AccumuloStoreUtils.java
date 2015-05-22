/**
 * 
 */
package mil.nga.giat.geowave.datastore.accumulo;

import mil.nga.giat.geowave.core.iface.store.StoreOperations;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.datastore.accumulo.mapreduce.JobContextAdapterStore;
import mil.nga.giat.geowave.datastore.accumulo.mapreduce.JobContextIndexStore;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloAdapterStore;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloDataStatisticsStore;

import org.apache.hadoop.mapreduce.JobContext;

/**
 * @author viggy
 * 
 */
public class AccumuloStoreUtils
{

	private static AccumuloStoreFactory m_accumuloStoreFactory;

	public static void setAccumuloStoreFactory(
			AccumuloStoreFactory accumuloStoreFactory ) {
		m_accumuloStoreFactory = accumuloStoreFactory;
	}

	public static JobContextIndexStore getJobContextIndexStore() {

		return m_accumuloStoreFactory.getJobContextIndexStore();
	}

	public static JobContextAdapterStore getJobContextAdapterStore(
			JobContext context ) {

		return m_accumuloStoreFactory.getJobContextAdapterStore(context);
	}

	public static JobContextAdapterStore getJobContextAdapterStore() {

		return m_accumuloStoreFactory.getJobContextAdapterStore();
	}

	public static AccumuloAdapterStore getAdapterStore(
			StoreOperations accumuloOperations ) {
		return m_accumuloStoreFactory.getAdapterStore(accumuloOperations);
	}

	public static IndexStore getIndexStore(
			JobContext context,
			StoreOperations accumuloOperations ) {
		return m_accumuloStoreFactory.getIndexStore(accumuloOperations);
	}

	public static AccumuloDataStatisticsStore getAccumuloDataStatisticsStore(
			StoreOperations accumuloOperations ) {
		return m_accumuloStoreFactory.getAccumuloDataStatisticsStore(accumuloOperations);
	}

}
