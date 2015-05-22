package mil.nga.giat.geowave.core.iface.combiner;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public interface ISortedKeyValueIterator<K extends WritableComparable<?>, V extends Writable>
{

	boolean hasTop();

	K getTopKey();

	V getTopValue();

	void next();

}
