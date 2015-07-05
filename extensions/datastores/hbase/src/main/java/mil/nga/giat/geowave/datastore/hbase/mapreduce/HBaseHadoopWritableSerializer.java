/**
 * 
 */
package mil.nga.giat.geowave.datastore.hbase.mapreduce;

import org.apache.hadoop.io.Writable;

/**
 * @author viggy
 * 
 */
public interface HBaseHadoopWritableSerializer<T, W extends Writable>
{
	public W toWritable(
			T entry );

	public T fromWritable(
			W writable );
}
