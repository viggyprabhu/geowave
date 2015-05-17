package mil.nga.giat.geowave.core.iface.field;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * @author viggy
 *
 */
public interface IKey  extends WritableComparable<IKey>{

	Text getColumnFamily();

}
