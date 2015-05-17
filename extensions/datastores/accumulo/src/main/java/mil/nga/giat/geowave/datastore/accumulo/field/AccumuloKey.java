/**
 * 
 */
package mil.nga.giat.geowave.datastore.accumulo.field;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import mil.nga.giat.geowave.core.iface.field.IKey;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.hadoop.io.Text;

/**
 * @author viggy
 *
 */
public class AccumuloKey implements IKey {

	private Key m_key;

	
	public AccumuloKey(){
		m_key = new Key();
	}

	public AccumuloKey(AccumuloKey topKey) {
		//TODO #238 Need to convert the existing AccumuloKey to a Key object of Accumulo Core
		m_key = new Key();
	}

	public AccumuloKey(Key replaceColumnVisibility) {
		m_key = replaceColumnVisibility;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		m_key.write(out);
		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		m_key.readFields(in);
	}

	@Override
	public int compareTo(IKey other) {
		return m_key.compareTo(new AccumuloKey().getKey());
	}

	@Override
	public Text getColumnFamily() {
		return m_key.getColumnFamily();
	}

	public ByteSequence getRowData() {
		return m_key.getRowData();
	}

	public Text getColumnVisibility() {
		return m_key.getColumnVisibility();
	}

	public Key getKey() {
		return m_key;
	}
	

}
