/**
 * 
 */
package mil.nga.giat.geowave.datastore.hbase.mapreduce.input;

/**
 * @author viggy
 * 
 */
public class GeoWaveHBaseInputSplit // extends InputSplit implements Writable
{

	/*
	 * private Map<Index, List<HBaseRange>> ranges; private String[] locations;
	 * 
	 * protected GeoWaveHBaseInputSplit() { ranges = new HashMap<Index,
	 * List<HBaseRange>>(); locations = new String[] {}; }
	 * 
	 * protected GeoWaveHBaseInputSplit( final Map<Index, List<HBaseRange>>
	 * ranges, final String[] locations ) { this.ranges = ranges; this.locations
	 * = locations; }
	 * 
	 * @Override public long getLength() throws IOException { long diff = 0; //
	 * TODO #406 Need to fix
	 * 
	 * for (final Entry<Index, List<HBaseRange>> indexEntry : ranges.entrySet())
	 * { for (final HBaseRange range : indexEntry.getValue()) { final Text
	 * startRow = range.isInfiniteStartKey() ? new Text( new byte[] {
	 * Byte.MIN_VALUE }) : new Text(range.getStartKey()); final Text stopRow =
	 * range.isInfiniteStopKey() ? new Text( new byte[] { Byte.MAX_VALUE }) :
	 * range.getEndKey(); final int maxCommon = Math.min( 7, Math.min(
	 * startRow.getLength(), stopRow.getLength()));
	 * 
	 * final byte[] start = startRow.getBytes(); final byte[] stop =
	 * stopRow.getBytes(); for (int i = 0; i < maxCommon; ++i) { diff |= 0xff &
	 * (start[i] ^ stop[i]); diff <<= Byte.SIZE; }
	 * 
	 * if (startRow.getLength() != stopRow.getLength()) { diff |= 0xff; } } }
	 * 
	 * return diff + 1; }
	 * 
	 * @Override public String[] getLocations() throws IOException { return
	 * locations; }
	 * 
	 * @Override public void readFields( final DataInput in ) throws IOException
	 * { final int numIndices = in.readInt(); ranges = new HashMap<Index,
	 * List<HBaseRange>>( numIndices); for (int i = 0; i < numIndices; i++) {
	 * final int indexLength = in.readInt(); final byte[] indexBytes = new
	 * byte[indexLength]; in.readFully(indexBytes); final Index index =
	 * PersistenceUtils.fromBinary( indexBytes, Index.class); final int
	 * numRanges = in.readInt(); final List<HBaseRange> rangeList = new
	 * ArrayList<HBaseRange>( numRanges);
	 * 
	 * for (int j = 0; j < numRanges; j++) { try { final HBaseRange range =
	 * HBaseRange.class.newInstance(); range.readFields(in);
	 * rangeList.add(range); } catch (InstantiationException |
	 * IllegalAccessException e) { throw new IOException(
	 * "Unable to instantiate range", e); } } ranges.put( index, rangeList); }
	 * final int numLocs = in.readInt(); locations = new String[numLocs]; for
	 * (int i = 0; i < numLocs; ++i) { locations[i] = in.readUTF(); } }
	 * 
	 * @Override public void write( final DataOutput out ) throws IOException {
	 * // TODO #406 Need to fix
	 * 
	 * out.writeInt(ranges.size()); for (final Entry<Index, List<Range>> range :
	 * ranges.entrySet()) { final byte[] indexBytes =
	 * PersistenceUtils.toBinary(range.getKey());
	 * out.writeInt(indexBytes.length); out.write(indexBytes); final List<Range>
	 * rangeList = range.getValue(); out.writeInt(rangeList.size()); for (final
	 * Range r : rangeList) { r.write(out); } } out.writeInt(locations.length);
	 * for (final String location : locations) { out.writeUTF(location); }
	 * 
	 * }
	 */
}
