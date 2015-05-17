/**
 * 
 */
package mil.nga.giat.geowave.core.iface.field;

/**
 * @author viggy
 * This interface is to give access to KVBuffer in Accumulo-core
 */
public interface IKVBuffer {

	void append(IKey topKey, IValue topValue);

}
