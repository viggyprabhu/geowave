package mil.nga.giat.geowave.accumulo.mapreduce;

import mil.nga.giat.geowave.store.adapter.DataAdapter;

import org.apache.hadoop.io.Writable;

public interface HadoopDataAdapter<T, W extends Writable> extends
		DataAdapter<T>
{
	public W toWritable(
			T entry );

	public T fromWritable(
			W writable );
}
