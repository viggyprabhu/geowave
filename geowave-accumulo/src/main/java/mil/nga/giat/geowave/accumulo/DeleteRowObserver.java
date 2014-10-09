package mil.nga.giat.geowave.accumulo;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

public interface DeleteRowObserver
{
	public void deleteRow(Key key, Value value);
}
