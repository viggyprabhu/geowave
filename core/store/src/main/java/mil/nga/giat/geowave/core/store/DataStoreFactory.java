/**
 * 
 */
package mil.nga.giat.geowave.core.store;

import mil.nga.giat.geowave.core.iface.combiner.IMergingCombiner;
import mil.nga.giat.geowave.core.iface.store.IJobContextAdapterStore;
import mil.nga.giat.geowave.core.iface.store.IJobContextIndexStore;
import mil.nga.giat.geowave.core.iface.store.StoreOperations;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.StoreException;
import mil.nga.giat.geowave.core.store.index.IndexStore;

import org.apache.hadoop.mapreduce.JobContext;

/**
 * @author viggy
 *
 */
public abstract class DataStoreFactory {

	private static DataStoreFactory m_dataStoreFactory;

	//TODO return AccumuloDataStoreFactory instance as of now
	public static DataStoreFactory getFactory() {
		return m_dataStoreFactory;
	}

	public abstract IMergingCombiner getMergedCombiner();

	public abstract StoreOperations getStoreOperations(String zookeeperURL,
			String instanceName, String userName, String password,
			String tableNamespace) throws StoreException;

	public abstract IndexStore getIndexStore(StoreOperations accumuloOperations);

	public abstract IJobContextIndexStore getJobContextIndexStore();
	
	public abstract AdapterStore getAdapterStore(
			StoreOperations accumuloOperations);

	public abstract IJobContextAdapterStore getJobContextAdapterStore(
			JobContext context) throws StoreException;

	public abstract IJobContextAdapterStore getJobContextAdapterStore();

}
