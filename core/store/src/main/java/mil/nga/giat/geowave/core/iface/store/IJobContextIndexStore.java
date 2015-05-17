/**
 * 
 */
package mil.nga.giat.geowave.core.iface.store;

import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.index.IndexStore;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;

/**
 * @author viggy
 *
 */
public interface IJobContextIndexStore extends IndexStore {

	public Index[] getIndices(JobContext context);

	public void addIndex(Configuration configuration, Index index);

}
