package mil.nga.giat.geowave.utils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import mil.nga.giat.geowave.accumulo.AbstractAccumuloPersistence;
import mil.nga.giat.geowave.accumulo.AccumuloAdapterStore;
import mil.nga.giat.geowave.accumulo.AccumuloDataStore;
import mil.nga.giat.geowave.accumulo.AccumuloIndexStore;
import mil.nga.giat.geowave.accumulo.AccumuloOperations;
import mil.nga.giat.geowave.accumulo.AccumuloUtils;
import mil.nga.giat.geowave.accumulo.BasicAccumuloOperations;
import mil.nga.giat.geowave.accumulo.CloseableIteratorWrapper;
import mil.nga.giat.geowave.accumulo.CloseableIteratorWrapper.ScannerClosableWrapper;
import mil.nga.giat.geowave.accumulo.AccumuloConstraintsQuery;
import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.index.StringUtils;
import mil.nga.giat.geowave.store.CloseableIterator;
import mil.nga.giat.geowave.store.adapter.AdapterStore;
import mil.nga.giat.geowave.store.adapter.DataAdapter;
import mil.nga.giat.geowave.store.filter.DedupeFilter;
import mil.nga.giat.geowave.store.filter.FilterList;
import mil.nga.giat.geowave.store.filter.QueryFilter;
import mil.nga.giat.geowave.store.index.Index;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

/**
 * 
    set splits on a table
    based on quantile distribution and fixed number of splits
        based on equal interval distribution and fixed number of splits
        based on fixed number of rows per split
    get all geowave namespaces
    set locality groups per column family (data adapter) or clear all locality groups
    get # of entries per data adapter in an index
    get # of entries per index
    get # of entries per namespace
    list adapters per namespace
    list indices per namespace

 * @author hayesrd1
 *
 */

public class GeowaveUtils
{
	private final static Logger LOGGER = Logger.getLogger(GeowaveUtils.class);
	
	private static boolean loaded;
	private static String zookeeperUrl;
	private static String instanceName;
	private static String geowaveUsername;
	private static String geowavePassword;
	
	public static void main(String[] args) {
		long time0 = System.nanoTime();
		System.out.println("getGeowaveNamespaces()");
		try {
//			getAllTablenames();
			
			for (String namespace : getGeowaveNamespaces()) {
				try {
					System.out.println("namespace: " + namespace);
					
//					foo(namespace);
					
//					long entries = getEntries(namespace);
//					System.out.println("number of entries: " + entries);
					Collection<Index> indices = getIndices(namespace);
					for (Index index : indices) {

					System.out.println("\tindex: " + StringUtils.stringFromBinary(index.getId().getBytes()));
					
					setSplitsByNumSplits(namespace, index, 10);
					
////						long entries = getEntries(namespace, index);
//						entries = getEntries(namespace, index);
//						System.out.println("\t\tnumber of entries: " + entries);
						Collection<DataAdapter<?>> adapters = getAdapters(namespace);
						for (DataAdapter<?> adapter : adapters) {
//							System.out.println("\t\tdata adapter: " + StringUtils.stringFromBinary(adapter.getAdapterId().getBytes()));
////							long entries = getEntries(namespace, index, adapter);
//							entries = getEntries(namespace, index, adapter);
//						    System.out.println("\t\t\tnumber of entries: " + entries);
//
//							boolean flag = isLocalityGroupSet(namespace, index, adapter);
//							System.out.println("\t\t\tlocality group: " + flag);
//							if (!flag) {
//								System.out.println("\t\t\tAdd locality group");
//								setLocalityGroup(namespace, index, adapter);
//								flag = isLocalityGroupSet(namespace, index, adapter);
//								System.out.println("\t\t\tlocality group: " + flag);
//							}
//							if (isLocalityGroupSet(namespace, index, adapter)) {
//								System.out.println("\t\t\tRemove locality group");
//								clearLocalityGroup(namespace, index, adapter);
//							}
						}
					}
					
					
				}
				catch (AccumuloException | AccumuloSecurityException | IOException | TableNotFoundException e) {
					LOGGER.error("Namespace = " + namespace + ": "+ e.getMessage());
				}
				catch(Exception e) {
					LOGGER.error("Possible Runtime Excpetion");
					LOGGER.error("Namespace = " + namespace + ": "+ e.getMessage());
				}
			}
		}
		catch (AccumuloException | AccumuloSecurityException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		long elapsed = System.nanoTime() - time0;
		long seconds = TimeUnit.SECONDS.convert(elapsed, TimeUnit.NANOSECONDS);
		System.out.println("Elapsed time (seconds): " + seconds);
	}
	
	public static void setSplitsByNumSplits(String namespace, Index index, int numberSplits) throws AccumuloException, AccumuloSecurityException, IOException, TableNotFoundException {
		long count = getEntries(namespace, index);
		
		CloseableIterator<Entry<Key,Value>> iterator = getIterator(namespace, index);

		long i = 0L;
		long count2 = 0L;
		long splitInterval = count / numberSplits;
		SortedSet<Text> splits = new TreeSet<Text>();
		while (iterator.hasNext()) {
			Entry<Key, Value> entry = iterator.next();
			i++;
			count2++;
			if (i >= splitInterval) {
				i = 0;
				splits.add(entry.getKey().getRow());
			}
		}
		System.out.println("count (alternative): " + count2);
		System.out.println("number of splits: " + splits.size());
		System.out.println("split interval: " + splitInterval);
		
//		BasicAccumuloOperations operations = getOperations(namespace);
//		String tableName = AccumuloUtils.getQualifiedTableName(namespace,
//				StringUtils.stringFromBinary(index.getId().getBytes()));
//		operations.getConnector().tableOperations().addSplits(tableName, splits);
//		operations.getConnector().tableOperations().compact(tableName, null, null, true, true);
	}
	
	public static void setSplitsByNumRows(String namespace, Index index, long numberRows) throws AccumuloException, AccumuloSecurityException, IOException, TableNotFoundException {
		long count = getEntries(namespace, index);
		
		CloseableIterator<Entry<Key,Value>> iterator = getIterator(namespace, index);

		long i = 0L;
		long count2 = 0L;
		SortedSet<Text> splits = new TreeSet<Text>();
		while (iterator.hasNext()) {
			Entry<Key, Value> entry = iterator.next();
			i++;
			count2++;
			if (i >= numberRows) {
				i = 0;
				splits.add(entry.getKey().getRow());
			}
		}
		System.out.println("count (alternative): " + count2);
		System.out.println("number of splits: " + splits.size());
		System.out.println("split interval: " + numberRows);
		
//		BasicAccumuloOperations operations = getOperations(namespace);
//		String tableName = AccumuloUtils.getQualifiedTableName(namespace,
//				StringUtils.stringFromBinary(index.getId().getBytes()));
//		operations.getConnector().tableOperations().addSplits(tableName, splits);
//		operations.getConnector().tableOperations().compact(tableName, null, null, true, true);
	}
	
	public static void setSplitsByPercentile(String namespace, Index index, int percentile) throws AccumuloException, AccumuloSecurityException, IOException, TableNotFoundException {
		CloseableIterator<Entry<Key,Value>> iterator = getIterator(namespace, index);

		Entry<Key, Value> first = null;
		Entry<Key, Value> last = null;
		
		while (iterator.hasNext()) {
			Entry<Key, Value> entry = iterator.next();
			if (first == null)
				first = entry;
			last = entry;
		}
		
		first.getKey().getRowData();
		last.getKey().getRowData();
		
//		System.out.println("count (alternative): " + count2);
//		System.out.println("number of splits: " + splits.size());
//		System.out.println("split interval: " + numberRows);
		
//		BasicAccumuloOperations operations = getOperations(namespace);
//		String tableName = AccumuloUtils.getQualifiedTableName(namespace,
//				StringUtils.stringFromBinary(index.getId().getBytes()));
//		operations.getConnector().tableOperations().addSplits(tableName, splits);
//		operations.getConnector().tableOperations().compact(tableName, null, null, true, true);
	}
	
//	public static void setSplits(String namespace, Index index) throws AccumuloException, AccumuloSecurityException, IOException, TableNotFoundException {
//		long count = getEntries(namespace, index);
//		
//		System.out.println("batch count: " + count);
//
//		AccumuloOperations operations = getOperations(namespace);
//		AccumuloIndexStore indexStore = new AccumuloIndexStore(operations);
//		AccumuloAdapterStore adapterStore = new AccumuloAdapterStore(operations);
//
//		if (indexStore.indexExists(index.getId())) {
//			String tableName = StringUtils.stringFromBinary(index.getId().getBytes());
//			final ScannerBase scanner = operations.createBatchScanner(tableName);
//			((BatchScanner) scanner).setRanges(AccumuloUtils.byteArrayRangesToAccumuloRanges(null));
//			
//			final IteratorSetting iteratorSettings = new IteratorSetting(
//					10,
//					"GEOWAVE_WHOLE_ROW_ITERATOR",
//					WholeRowIterator.class);
//			scanner.addScanIterator(iteratorSettings);
//			
//			List<QueryFilter> clientFilters = new ArrayList<QueryFilter>();
//			clientFilters.add(
//					0,
//					new DedupeFilter());
//			
//			Iterator<Entry<Key,Value>> it = new IteratorWrapper(adapterStore, index, scanner.iterator(), new FilterList<QueryFilter>(
//					clientFilters));
//			
//			CloseableIterator<Entry<Key,Value>> iterator = new CloseableIteratorWrapper<Entry<Key,Value>>(new ScannerClosableWrapper(scanner), it);
//
//			long i = 0L;
//			long count2 = 0L;
//			double splitCount = 12;
//			double splitInterval = count / splitCount;
//			SortedSet<Text> splits = new TreeSet<Text>();
//			while (iterator.hasNext()) {
//				Entry<Key, Value> entry = iterator.next();
//				i++;
//				count2++;
//				if (i > splitInterval) {
//					i = 0;
//					splits.add(entry.getKey().getRow());
//				}
//			}
//			System.out.println("count (alternative): " + count2);
//			System.out.println("number of splits: " + splits.size());
//			System.out.println("split interval: " + splitInterval);
//		}
//		
//		// operations.getConnector().tableOperations().addSplits(tableName, splits);
//		//operations.getConnector().tableOperations().compact(tableName, null, null, true, true);
//	}
	
	/**
	 * Get GeoWave Namespaces
	 * 
	 * @return
	 * @throws AccumuloException
	 * @throws AccumuloSecurityException
	 * @throws IOException
	 */
	public static Collection<String> getGeowaveNamespaces() throws AccumuloException, AccumuloSecurityException, IOException {
		List<String> namespaces = new ArrayList<String>();

		TableOperations tableOperations = getOperations("").getConnector().tableOperations();
		List<String> allTables = new ArrayList<String>();
		allTables.addAll(tableOperations.list());

		for (String table : allTables) {
			if (table.contains(AbstractAccumuloPersistence.METADATA_TABLE)) {
				String namespace = table.substring(0, table.indexOf("_" + AbstractAccumuloPersistence.METADATA_TABLE));
				if (!namespaces.contains(namespace))
					namespaces.add(namespace);
			}
		}
		return namespaces;
	}
	
	/**
	 * Check if locality group is set.
	 * 
	 * @param namespace
	 * @param index
	 * @param adapter
	 * @return
	 * @throws AccumuloException
	 * @throws AccumuloSecurityException
	 * @throws IOException
	 * @throws TableNotFoundException
	 */
	public static boolean isLocalityGroupSet(String namespace, Index index, DataAdapter<?> adapter) throws AccumuloException, AccumuloSecurityException, IOException, TableNotFoundException {
		AccumuloOperations operations = getOperations(namespace);
		// get unqualified table name
		String tableName = StringUtils.stringFromBinary(index.getId().getBytes());
		return operations.localityGroupExists(tableName, adapter.getAdapterId().getBytes());
	}
	
	/**
	 * Set locality group.
	 * 
	 * @param namespace
	 * @param index
	 * @param adapter
	 * @throws AccumuloException
	 * @throws AccumuloSecurityException
	 * @throws IOException
	 * @throws TableNotFoundException
	 */
	public static void setLocalityGroup(String namespace, Index index, DataAdapter<?> adapter) throws AccumuloException, AccumuloSecurityException, IOException, TableNotFoundException {
		AccumuloOperations operations = getOperations(namespace);
		// get unqualified table name
		String tableName = StringUtils.stringFromBinary(index.getId().getBytes());
		operations.addLocalityGroup(tableName, adapter.getAdapterId().getBytes());
	}
	
	/**
	 * Clear locality group.
	 * 
	 * @param namespace
	 * @param index
	 * @param adapter
	 * @throws AccumuloException
	 * @throws AccumuloSecurityException
	 * @throws IOException
	 * @throws TableNotFoundException
	 */
	public static void clearLocalityGroup(String namespace, Index index, DataAdapter<?> adapter) throws AccumuloException, AccumuloSecurityException, IOException, TableNotFoundException {
		AccumuloOperations operations = getOperations(namespace);
		// get unqualified table name
		String tableName = StringUtils.stringFromBinary(index.getId().getBytes());
		operations.clearLocalityGroup(tableName, adapter.getAdapterId().getBytes());
	}
	
	/**
	 * Get number of entries for a data adapter in an index.
	 * 
	 * @param namespace
	 * @param index
	 * @param adapter
	 * @return
	 * @throws AccumuloException
	 * @throws AccumuloSecurityException
	 * @throws IOException
	 */
	public static long getEntries(String namespace, Index index, DataAdapter<?> adapter) throws AccumuloException, AccumuloSecurityException, IOException {
		long counter = 0L;
		AccumuloOperations operations = getOperations(namespace);
		AccumuloIndexStore indexStore = new AccumuloIndexStore(operations);
		AccumuloAdapterStore adapterStore = new AccumuloAdapterStore(operations);
		if (indexStore.indexExists(index.getId()) && adapterStore.adapterExists(adapter.getAdapterId())) {
			List<ByteArrayId> adapterIds = new ArrayList<ByteArrayId>();
			adapterIds.add(adapter.getAdapterId());
			AccumuloConstraintsQuery accumuloQuery = new AccumuloConstraintsQuery(adapterIds, index);
			CloseableIterator<?> iterator = accumuloQuery.query(operations, new AccumuloAdapterStore(operations), null);
			while (iterator.hasNext()) {
				counter++;
				iterator.next();
			}
			iterator.close();
		}
		return counter;
	}

	/**
	 * Get number of entries per index.
	 * 
	 * @param namespace
	 * @param index
	 * @return
	 * @throws AccumuloException
	 * @throws AccumuloSecurityException
	 * @throws IOException
	 */
	public static long getEntries(String namespace, Index index) throws AccumuloException, AccumuloSecurityException, IOException {
		long counter = 0L;
		AccumuloOperations operations = getOperations(namespace);
		AccumuloIndexStore indexStore = new AccumuloIndexStore(operations);
		if (indexStore.indexExists(index.getId())) {
			AccumuloConstraintsQuery accumuloQuery = new AccumuloConstraintsQuery(index);
			CloseableIterator<?> iterator = accumuloQuery.query(operations, new AccumuloAdapterStore(operations), null);
			while (iterator.hasNext()) {
				counter++;
				iterator.next();
			}
			iterator.close();
		}
		return counter;
	}

	/**
	 * Get number of entries per namespace.
	 * 
	 * @param namespace
	 * @return
	 * @throws AccumuloException
	 * @throws AccumuloSecurityException
	 * @throws IOException
	 */
	public static long getEntries(String namespace) throws AccumuloException, AccumuloSecurityException, IOException {
		long counter = 0L;
		AccumuloDataStore dataStore = getDataStore(namespace);
		if (dataStore != null) {
			CloseableIterator<?> iterator = dataStore.query(null);
			while (iterator.hasNext()) {
				counter++;
				iterator.next();
			}
			iterator.close();
		}
		return counter;
	}

	/**
	 * List of data adapters associated with a GeoWave namespace.
	 * @param namespace
	 * @return
	 * @throws AccumuloException
	 * @throws AccumuloSecurityException
	 * @throws IOException
	 * @throws TableNotFoundException
	 */
	public static Collection<DataAdapter<?>> getAdapters(String namespace) throws AccumuloException, AccumuloSecurityException, IOException, TableNotFoundException {
		List<DataAdapter<?>> adapters = new ArrayList<DataAdapter<?>>();
	
		AccumuloDataStore dataStore = getDataStore(namespace);
		if (dataStore != null) {
			AdapterStore adapterStore = dataStore.getAdapterStore();
			if (adapterStore instanceof AccumuloAdapterStore) {
				Iterator<DataAdapter<?>> iterator = ((AccumuloAdapterStore)adapterStore).getAdapters();
				while (iterator.hasNext()) {
					adapters.add(iterator.next());
				}
			}
		}
		return adapters;
	}

	/**
	 * List of indices for a namespace.
	 * 
	 * @param namespace
	 * @return
	 * @throws AccumuloException
	 * @throws AccumuloSecurityException
	 * @throws IOException
	 * @throws TableNotFoundException
	 */
	public static Collection<Index> getIndices(String namespace) throws AccumuloException, AccumuloSecurityException, IOException, TableNotFoundException {
		List<Index> indices = new ArrayList<Index>();
	
		AccumuloDataStore dataStore = getDataStore(namespace);
		if (dataStore != null) {
			Iterator<Index> iterator = dataStore.getIndexStore().getIndices();
			while (iterator.hasNext()) {
				indices.add(iterator.next());
			}
		}
		return indices;
	}

	/**
	 * Load GeoWave connection properties.
	 * 
	 * @throws IOException
	 */
	private static void loadProperties() throws IOException {
		if (!loaded) {
			Properties prop = new Properties();
			String propFileName = "mil/nga/giat/geowave/utils/config.properties";

			InputStream inputStream = GeowaveUtils.class.getClassLoader().getResourceAsStream(propFileName);
			prop.load(inputStream);
			if (inputStream == null) {
				throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
			}

			zookeeperUrl = prop.getProperty("zookeeperUrl");
			instanceName = prop.getProperty("instanceName");
			geowaveUsername = prop.getProperty("geowave_username");
			geowavePassword = prop.getProperty("geowave_password");

			loaded = true;
		}
	}

	/**
	 * Create accumulo connection.
	 * 
	 * @param namespace
	 * @return
	 * @throws AccumuloException
	 * @throws AccumuloSecurityException
	 * @throws IOException
	 */
	private static BasicAccumuloOperations getOperations(String namespace) throws AccumuloException, AccumuloSecurityException, IOException {
		loadProperties();

		return new BasicAccumuloOperations(zookeeperUrl,
				instanceName,
				geowaveUsername,
				geowavePassword,
				namespace);
	}

	/**
	 * Create accumulo version of the data store.
	 * 
	 * @param namespace
	 * @return
	 * @throws AccumuloException
	 * @throws AccumuloSecurityException
	 * @throws IOException
	 */
	private static AccumuloDataStore getDataStore(String namespace) throws AccumuloException, AccumuloSecurityException, IOException {
		return new AccumuloDataStore(getOperations(namespace));
	}
	
private static CloseableIterator<Entry<Key,Value>> getIterator(String namespace, Index index) throws AccumuloException, AccumuloSecurityException, IOException, TableNotFoundException {
		CloseableIterator<Entry<Key,Value>> iterator = null;
		AccumuloOperations operations = getOperations(namespace);
		AccumuloIndexStore indexStore = new AccumuloIndexStore(operations);
		AccumuloAdapterStore adapterStore = new AccumuloAdapterStore(operations);
	
		if (indexStore.indexExists(index.getId())) {
			String tableName = StringUtils.stringFromBinary(index.getId().getBytes());
			final ScannerBase scanner = operations.createBatchScanner(tableName);
			((BatchScanner) scanner).setRanges(AccumuloUtils.byteArrayRangesToAccumuloRanges(null));
			
			final IteratorSetting iteratorSettings = new IteratorSetting(
					10,
					"GEOWAVE_WHOLE_ROW_ITERATOR",
					WholeRowIterator.class);
			scanner.addScanIterator(iteratorSettings);
			
			List<QueryFilter> clientFilters = new ArrayList<QueryFilter>();
			clientFilters.add(
					0,
					new DedupeFilter());
			
			Iterator<Entry<Key,Value>> it = new IteratorWrapper(adapterStore, index, scanner.iterator(), new FilterList<QueryFilter>(
					clientFilters));
			
			iterator = new CloseableIteratorWrapper<Entry<Key,Value>>(new ScannerClosableWrapper(scanner), it);
		}
		return iterator;
	}

//	private static Collection<String> getIndexTableNames(String namespace) throws AccumuloException, AccumuloSecurityException, IOException {
//		Collection<String> tableNames = new ArrayList<String>();
//		for (String tableName : getTableNames(namespace)) {
//			if (!tableName.contains(AbstractAccumuloPersistence.METADATA_TABLE))
//				tableNames.add(tableName);
//		}
//		return tableNames;
//	}
//	
//	private static Collection<String> getMetaTableNames(String namespace) throws AccumuloException, AccumuloSecurityException, IOException {
//		Collection<String> tableNames = new ArrayList<String>();
//		for (String tableName : getTableNames(namespace)) {
//			if (tableName.contains(AbstractAccumuloPersistence.METADATA_TABLE))
//				tableNames.add(tableName);
//		}
//		return tableNames;
//	}
//	
//	private static Collection<String> getTableNames(String namespace) throws AccumuloException, AccumuloSecurityException, IOException {
//		Collection<String> tableNames = new ArrayList<String>();
//		BasicAccumuloOperations operations = getOperations(namespace);
//		Connector connector = operations.getConnector();
//
//		for (String tableName : connector.tableOperations().list()) {
//			if (tableName.startsWith(namespace)) {
//				tableNames.add(tableName);
//			}
//		}
//		return tableNames;
//	}
//
//	private static void getMetaTableInfo(String namespace) throws AccumuloException, AccumuloSecurityException, TableNotFoundException, IOException {
//		BasicAccumuloOperations operations = getOperations(namespace);
//		Connector connector = operations.getConnector();
//
//		for (String tableName : connector.tableOperations().list()) {
//			if (tableName.startsWith(namespace)
//					&& tableName.contains(AbstractAccumuloPersistence.METADATA_TABLE)) {
//				Scanner scanner = operations.createScanner(tableName.replace(namespace + "_", ""));
//
//				System.out.println("Table name: " + tableName);
//				
//				List<String> colFamily = new ArrayList<String>();
//				for (Entry<Key, Value> entry : scanner) {
//					String family = entry.getKey().getColumnFamily().toString();
//					if (!colFamily.contains(family)) {
//						colFamily.add(family);
//						System.out.println("\tColumn Family: " + family);
//					}
//				}
//			}
//		}
//	}
//	
//	private static void getTableInfo(String namespace) throws AccumuloException, AccumuloSecurityException, TableNotFoundException, IOException {
//		BasicAccumuloOperations operations;
//		operations = getOperations(namespace);
//		Connector connector = operations.getConnector();
//
//		for (String tableName : connector.tableOperations().list()) {
//			if (tableName.startsWith(namespace)
//					&& !tableName.contains(AbstractAccumuloPersistence.METADATA_TABLE)) {
//				Scanner scanner = operations.createScanner(tableName.replace(namespace + "_", ""));
//
//				System.out.println("Table name: " + tableName);
//				
//				List<String> colFamily = new ArrayList<String>();
//				for (Entry<Key, Value> entry : scanner) {
//					String family = entry.getKey().getColumnFamily().toString();
//					if (!colFamily.contains(family)) {
//						colFamily.add(family);
//						System.out.println("\tColumn Family: " + family);
//					}
//				}
//			}
//		}
//	}
//
//	private static void foo(String namespace) throws AccumuloException, AccumuloSecurityException, IOException, TableNotFoundException {
//		TableOperations operations = getOperations(namespace).getConnector().tableOperations();
//		Collection<String> tables = getTableNames(namespace);
//		for (String tableName : tables) {
//			Collection<Text> splits = operations.listSplits(tableName);
//			int numberOfSplits = 0;
//			for(Text split : splits) {
//				numberOfSplits++;
//				System.out.println("key: " + split.toString());
//			}
//			System.out.println("Number of Splits (" + tableName + ") :" + numberOfSplits);
//		}
//	}

//	public static void getAllTablenames() throws AccumuloException, AccumuloSecurityException, IOException {
//		TableOperations tableOperations = getOperations("").getConnector().tableOperations();
//		List<String> allTables = new ArrayList<String>();
//		allTables.addAll(tableOperations.list());
//
//		for (String table : allTables) {
//			System.out.println("tablename : " + table);
//		}
//	}
	
	static class IteratorWrapper implements Iterator<Entry<Key, Value>> {

		private final Iterator<Entry<Key, Value>> scannerIt;
		private AdapterStore adapterStore;
		private Index index;
		private QueryFilter clientFilter;
		private Entry<Key, Value> nextValue;
		
		public IteratorWrapper(
				final AdapterStore adapterStore,
				final Index index,
				final Iterator<Entry<Key, Value>> scannerIt,
				final QueryFilter clientFilter ) {
			this.adapterStore = adapterStore;
			this.index = index;
			this.scannerIt = scannerIt;
			this.clientFilter = clientFilter;
			findNext();
		}
		
		private void findNext() {
			while (scannerIt.hasNext()) {
				final Entry<Key, Value> row = scannerIt.next();
				final Object decodedValue = decodeRow(
						row,
						clientFilter,
						index);
				if (decodedValue != null) {
					nextValue = row;
					return;
				}
			}
			nextValue = null;
		}
		
		private Object decodeRow(
				final Entry<Key, Value> row,
				final QueryFilter clientFilter,
				final Index index ) {
			return AccumuloUtils.decodeRow(
					row.getKey(),
					row.getValue(),
					null,
					adapterStore,
					clientFilter,
					index);
		}

		@Override
		public boolean hasNext() {
			return nextValue != null;
		}

		@Override
		public Entry<Key, Value> next() {
			final Entry<Key, Value> previousNext = nextValue;
			findNext();
			return previousNext;
		}

		@Override
		public void remove() {
			// TODO Auto-generated method stub
			
		}
	}
	
//	class IteratorWrapper<Entry<Key, Value>> implements
//	Iterator<Entry<Key, Value>>
//	{
//		private final Iterator<Entry<Key, Value>> scannerIt;
//
//		private T nextValue;
//
//		public IteratorWrapper(
//				final Iterator<Entry<Key, Value>> scannerIt) {
//			this.scannerIt = scannerIt;
//		}
//
//		@Override
//		public boolean hasNext() {
//			return scannerIt.hasNext();
//		}
//
//		@Override
//		public Entry<Key, Value> next() {
//			return scannerIt.next();
//		}
//
//		@Override
//		public void remove() {
//			// TODO what should we do here considering the scanning iterator is
//			// already past the current entry? it probably doesn't matter much as
//			// this is not called in practice
//
//			// scannerIt.remove();
//		}
//
//}


}
