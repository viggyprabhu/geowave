/**
 * 
 */
package mil.nga.giat.geowave.core.iface.store;

import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;

/**
 * @author viggy
 * 
 */
public interface IJobContextAdapterStore extends
		AdapterStore
{

	DataAdapter[] getDataAdapters(
			JobContext context );

	void addDataAdapter(
			Configuration configuration,
			DataAdapter<?> newAdapter );

}
