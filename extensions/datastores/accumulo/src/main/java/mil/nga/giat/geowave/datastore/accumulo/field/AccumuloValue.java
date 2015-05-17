/**
 * 
 */
package mil.nga.giat.geowave.datastore.accumulo.field;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import mil.nga.giat.geowave.core.iface.field.IValue;

import org.apache.accumulo.core.data.Value;

/**
 * @author viggy
 *
 */
public class AccumuloValue implements IValue {

	Value m_value;
	
	public AccumuloValue(){
		m_value = new Value();
	}
	
	public AccumuloValue(byte[] binary) {
		m_value = new Value(binary);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		m_value.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		m_value.readFields(in);
	}

	@Override
	public int compareTo(Object o) {
		return m_value.compareTo(o);
	}

	public byte[] get() {
		// TODO Auto-generated method stub
		return null;
	}

}
