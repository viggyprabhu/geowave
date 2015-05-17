/**
 * 
 */
package mil.nga.giat.geowave.datastore.accumulo;

import mil.nga.giat.geowave.core.iface.combiner.IMergingCombiner;
import mil.nga.giat.geowave.core.iface.store.StoreOperations;
import mil.nga.giat.geowave.core.store.DataStoreFactory;
import mil.nga.giat.geowave.core.store.adapter.StoreException;
import mil.nga.giat.geowave.datastore.accumulo.mapreduce.JobContextAdapterStore;
import mil.nga.giat.geowave.datastore.accumulo.mapreduce.JobContextIndexStore;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloAdapterStore;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloIndexStore;

import org.apache.hadoop.mapreduce.JobContext;

/**
 * @author viggy
 *
 */
public class AccumuloStoreFactory extends DataStoreFactory {

	public AccumuloStoreFactory(){
		AccumuloStoreUtils.setAccumuloStoreFactory(this);
	}
	
	@Override
	public IMergingCombiner getMergedCombiner() {
		
		return new MergingCombiner();
	}

	@Override
	public StoreOperations getStoreOperations(String zookeeperURL,
			String instanceName, String userName, String password,
			String tableNamespace) throws StoreException {
		// TODO #238 Auto-generated method stub
		return null;
	}

	@Override
	public AccumuloIndexStore getIndexStore(StoreOperations accumuloOperations) {
		// TODO #238 Auto-generated method stub
		return null;
	}

	@Override
	public AccumuloAdapterStore getAdapterStore(
			StoreOperations accumuloOperations) {
		// TODO #238 Auto-generated method stub
		return null;
	}

	@Override
	public JobContextIndexStore getJobContextIndexStore() {
		// TODO #238 Auto-generated method stub
		return null;
	}

	@Override
	public JobContextAdapterStore getJobContextAdapterStore(JobContext context) {
		// TODO #238 Auto-generated method stub
		return null;
	}

	@Override
	public JobContextAdapterStore getJobContextAdapterStore() {
		// TODO #238 Need to ensure that only one of the getJobContextAdapterStore is exposed
		return null;
	}

}
