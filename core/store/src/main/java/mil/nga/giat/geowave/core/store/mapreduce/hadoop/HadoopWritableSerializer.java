package mil.nga.giat.geowave.core.store.mapreduce.hadoop;

import org.apache.hadoop.io.Writable;

/**
 * 
 * @param <T>
 *            the native type
 * @param <W>
 *            the writable type
 * 
 */
public interface HadoopWritableSerializer<T, W extends Writable>
{
	public W toWritable(
			T entry );

	public T fromWritable(
			W writable );
}
