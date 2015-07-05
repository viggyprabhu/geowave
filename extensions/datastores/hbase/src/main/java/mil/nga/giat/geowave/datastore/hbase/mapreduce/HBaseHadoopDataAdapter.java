/**
 * 
 */
package mil.nga.giat.geowave.datastore.hbase.mapreduce;

import mil.nga.giat.geowave.core.store.adapter.DataAdapter;

import org.apache.hadoop.io.Writable;

/**
 * @author viggy
 * 
 */
public interface HBaseHadoopDataAdapter<T, W extends Writable> extends
		DataAdapter<T>
{
	public HBaseHadoopWritableSerializer<T, W> createWritableSerializer();
}
