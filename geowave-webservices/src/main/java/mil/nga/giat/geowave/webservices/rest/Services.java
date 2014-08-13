package mil.nga.giat.geowave.webservices.rest;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

import mil.nga.giat.geowave.accumulo.AbstractAccumuloPersistence;
import mil.nga.giat.geowave.accumulo.AccumuloAdapterStore;
import mil.nga.giat.geowave.accumulo.AccumuloDataStore;
import mil.nga.giat.geowave.accumulo.AccumuloIndexStore;
import mil.nga.giat.geowave.accumulo.BasicAccumuloOperations;
import mil.nga.giat.geowave.gt.adapter.FeatureDataAdapter;
import mil.nga.giat.geowave.index.PersistenceUtils;
import mil.nga.giat.geowave.store.adapter.AdapterStore;
import mil.nga.giat.geowave.store.adapter.DataAdapter;
import mil.nga.giat.geowave.store.index.Index;
import mil.nga.giat.geowave.store.index.IndexStore;
import mil.nga.giat.geowave.webservices.rest.data.BoundingBox;
import mil.nga.giat.geowave.webservices.rest.data.DataStore;
import mil.nga.giat.geowave.webservices.rest.data.Layer;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.opengis.feature.simple.SimpleFeatureType;

@Path("/services")
public class Services
{
	@Context
	private static HttpServletRequest request;

	public static void main(String [] args) {
	}

	@GET
	@Path("/geowaveNamespaces")
	public static Response getGeowaveNamespaces() {
		JSONObject obj = new JSONObject();
		try {
			List<String> namespaces = new ArrayList<String>();

			Connector connector;

			connector = getOperations("").getConnector();

			TableOperations tableOperations = connector.tableOperations();
			List<String> allTables = new ArrayList<String>();
			allTables.addAll(tableOperations.list());

			for (String table : allTables) {
				if (table.contains("_GEOWAVE_METADATA")) {
					String namespace = table.substring(0, table.indexOf("_GEOWAVE_METADATA"));
					if (!namespaces.contains(namespace))
						namespaces.add(namespace);
				}
			}

			obj.put("zookeeperUrl", zookeeperUrl);
			obj.put("instanceName", instanceName);
			obj.put("username", geowaveUsername);
			obj.put("password", geowavePassword);
			obj.put("namespaces", namespaces);
		}
		catch (AccumuloException | AccumuloSecurityException | JSONException | IOException e) {
			LOGGER.error(e.getMessage());
			return Response.status(500).entity(e.getMessage()).build();
		}
		return Response.status(200).entity(obj.toString()).build();
	}

	@GET
	@Path("/geowaveLayers/{namespace}")
	public static Response getGeowaveLayers(@PathParam("namespace")String namespace) {
		JSONObject obj = new JSONObject();
		try {
			List<String> layers = new ArrayList<String>();

			AccumuloDataStore dataStore = getDataStore(namespace);
			IndexStore indexStore = dataStore.getIndexStore();
			AdapterStore adapterStore = dataStore.getAdapterStore();
			if (indexStore instanceof AccumuloIndexStore) {
				Iterator<Index> indexIter = ((AccumuloIndexStore)indexStore).getIndices();
				while(indexIter.hasNext()) {
					indexIter.next();
					if (adapterStore instanceof AccumuloAdapterStore) {
						Iterator<DataAdapter<?>> iterator = ((AccumuloAdapterStore)adapterStore).getAdapters();
						while (iterator.hasNext()) {
							DataAdapter<?> dataAdapter = iterator.next();
							if (dataAdapter instanceof FeatureDataAdapter) {
								SimpleFeatureType simpleFeatureType = ((FeatureDataAdapter)dataAdapter).getType();
								layers.add(simpleFeatureType.getTypeName());
							}
						}
					}
				}
			}
			obj.put("namespace", namespace);
			obj.put("layers", layers);
		}
		catch (AccumuloException | AccumuloSecurityException | TableNotFoundException | JSONException | IOException e) {
			LOGGER.error(e.getMessage());
			return Response.status(500).entity(e.getMessage()).build();
		}
		return Response.status(200).entity(obj.toString()).build();
	}

	public static void publishDataStore(String name) {
		publishDataStore(name, name);
	}
	
	public static void publishDataStore(String name, String namespace) {
		try {
			DataStore ds = new DataStore();
			ds.setName(name);
			ds.setType("GeoWave DataStore");
			ds.setEnabled(true);

			Map<String , String> ws = new HashMap<String, String>();
			ws.put("name","geowave");
			ws.put("href", geoserverRestUrl + "/workspaces/geowave.json");
			ds.setWorkspace(ws);

			Map<String, String> cp = new HashMap<String, String>();
			cp.put("ZookeeperServers", zookeeperUrl);
			cp.put("Password", geowavePassword);
			cp.put("Namespace", namespace);
			cp.put("UserName", geowaveUsername);
			cp.put("InstanceName", instanceName);

			ds.setConnectionParameters(cp);

			ds.set_default(false);
			ds.setFeatureTypes(geoserverRestUrl + "/workspaces/geowave/datastores/"
					+ namespace + "/featuretypes.json");
			
			String input = ds.toJSONString();
			LOGGER.debug("GeoServer rest input: " + input);
			
			URL url = new URL(geoserverRestUrl + "/workspaces/geowave/datastores.json");
			
			HttpURLConnection connection =
					(HttpURLConnection) url.openConnection();
			
			String userpass = geoserverUsername + ":" + geoserverPassword;
			@SuppressWarnings("restriction")
			String basicAuth = "Basic " + new String(new sun.misc.BASE64Encoder().encode(userpass.getBytes()));
			connection.setRequestProperty ("Authorization", basicAuth);
			
			connection.setDoOutput(true);
			connection.setRequestMethod("POST");
			connection.setRequestProperty("Content-Type", "application/json");
			connection.setRequestProperty("Accept", "application/json");
			
			OutputStream os = connection.getOutputStream();
			os.write(input.getBytes());
			
			os.flush();
			os.close();
			
			if (connection.getResponseCode() != HttpURLConnection.HTTP_CREATED) {
				LOGGER.error("Failed : HTTP error code : " + connection.getResponseCode());
				throw new RuntimeException("Failed : HTTP error code : "
						+ connection.getResponseCode());
			}

			BufferedReader br = new BufferedReader(new InputStreamReader(
					(connection.getInputStream())));

			String output;
			LOGGER.info("publishDataStore() response code: " + connection.getResponseCode());
			while ((output = br.readLine()) != null) {
				LOGGER.info("Response from GeoServer: " + output);
			}

			connection.disconnect();
		}
		catch (IOException | JSONException e) {
			LOGGER.error(e.getMessage());
		}
	}

	public static void publishLayer(String dataStore, String layerName) {
		try {
			Layer layer = new Layer();

			layer.setName(layerName);
			layer.setNativeName(layerName);
			
			Map<String, String> namespace = new LinkedHashMap<String, String>();
			namespace.put("name", "geowave");
			namespace.put("href", geoserverRestUrl + "/namespaces/geowave.json");
			layer.setNamespace(namespace);
			
			layer.setTitle(layerName);
			layer.setDescription("GeoWave Resource");

			List<Map<String, String>> keywords = new ArrayList<Map<String, String>>();
			Map<String, String> temp = new LinkedHashMap<String, String>();
			temp.put("string", layerName);
			keywords.add(temp);
			layer.setKeywords(keywords);

			layer.setNativeCRS("GEOGCS[\"WGS 84\", \n  DATUM[\"World Geodetic System 1984\", \n    SPHEROID[\"WGS 84\", 6378137.0, 298.257223563, AUTHORITY[\"EPSG\",\"7030\"]], \n    AUTHORITY[\"EPSG\",\"6326\"]], \n  PRIMEM[\"Greenwich\", 0.0, AUTHORITY[\"EPSG\",\"8901\"]], \n  UNIT[\"degree\", 0.017453292519943295], \n  AXIS[\"Geodetic longitude\", EAST], \n  AXIS[\"Geodetic latitude\", NORTH], \n  AUTHORITY[\"EPSG\",\"4326\"]]");
			layer.setSrs("EPSG:4326");
			
			// TODO Default values for bounding box. Need to calculate actual min and max based upon data
			BoundingBox _native = new BoundingBox(-180,180,-90,90,"EPSG:4326");
			layer.setNative(_native);
			BoundingBox latLon = new BoundingBox(-180,180,-90,90,"GEOGCS[\"WGS84(DD)\", \n  DATUM[\"WGS84\", \n    SPHEROID[\"WGS84\", 6378137.0, 298.257223563]], \n  PRIMEM[\"Greenwich\", 0.0], \n  UNIT[\"degree\", 0.017453292519943295], \n  AXIS[\"Geodetic longitude\", EAST], \n  AXIS[\"Geodetic latitude\", NORTH]]");
			layer.setLatLon(latLon);

			layer.setProjectionPolicy("FORCE_DECLARED");

			layer.setEnabled(true);
			
			Map<String, String> i_map = new LinkedHashMap<String, String>();
			i_map.put("@key", "cachingEnabled");
			i_map.put("$", "false");
			Map<String, Map<String, String>> o_map = new LinkedHashMap<String, Map<String, String>>();
			o_map.put("entry", i_map);
			List<Map<String, Map<String, String>>> metadata = new ArrayList<Map<String, Map<String, String>>>();
			metadata.add(o_map);
			layer.setMetadata(metadata);
			
			Map<String, String> store = new LinkedHashMap<String, String>();
			store.put("@class", "dataStore");
			store.put("name", dataStore);
			store.put("href", geoserverRestUrl + "/workspaces/geowave/datastores/" + dataStore + ".json");
			layer.setStore(store);
			
			// TODO The following values are based upon the attributes of the layer. The attributes need to be calculated
			Map<String, Object> geometry = new LinkedHashMap<String, Object>();
			geometry.put("name", "geometry");
			geometry.put("minOccurs", 0);
			geometry.put("maxOccurs", 1);
			geometry.put("nillable", true);
			geometry.put("binding", "com.vividsolutions.jts.geom.Geometry");
			
			Map<String, Object> timestamp = new LinkedHashMap<String, Object>();
			timestamp.put("name", "StartTimeStamp");
			timestamp.put("minOccurs", 0);
			timestamp.put("maxOccurs", 1);
			timestamp.put("nillable", true);
			timestamp.put("binding", "java.util.Date");
			
			Map<String, Object> etimestamp = new LinkedHashMap<String, Object>();
			etimestamp.put("name", "EndTimeStamp");
			etimestamp.put("minOccurs", 0);
			etimestamp.put("maxOccurs", 1);
			etimestamp.put("nillable", true);
			etimestamp.put("binding", "java.util.Date");
			
			Map<String, Object> duration = new LinkedHashMap<String, Object>();
			duration.put("name", "Duration");
			duration.put("minOccurs", 0);
			duration.put("maxOccurs", 1);
			duration.put("nillable", true);
			duration.put("binding", "java.lang.Long");
			
			Map<String, Object> numPoints = new LinkedHashMap<String, Object>();
			numPoints.put("name", "NumberPoints");
			numPoints.put("minOccurs", 0);
			numPoints.put("maxOccurs", 1);
			numPoints.put("nillable", true);
			numPoints.put("binding", "java.lang.Long");
			
			Map<String, Object> trackId = new LinkedHashMap<String, Object>();
			trackId.put("name", "TrackId");
			trackId.put("minOccurs", 0);
			trackId.put("maxOccurs", 1);
			trackId.put("nillable", true);
			trackId.put("binding", "java.lang.String");
			
			Map<String, Object> userId = new LinkedHashMap<String, Object>();
			userId.put("name", "UserId");
			userId.put("minOccurs", 0);
			userId.put("maxOccurs", 1);
			userId.put("nillable", true);
			userId.put("binding", "java.lang.Long");
			
			Map<String, Object> user = new LinkedHashMap<String, Object>();
			user.put("name", "User");
			user.put("minOccurs", 0);
			user.put("maxOccurs", 1);
			user.put("nillable", true);
			user.put("binding", "java.lang.String");

			Map<String, Object> desc = new LinkedHashMap<String, Object>();
			desc.put("name", "Description");
			desc.put("minOccurs", 0);
			desc.put("maxOccurs", 1);
			desc.put("nillable", true);
			desc.put("binding", "java.lang.String");

			Map<String, Object> tag = new LinkedHashMap<String, Object>();
			tag.put("name", "Tags");
			tag.put("minOccurs", 0);
			tag.put("maxOccurs", 1);
			tag.put("nillable", true);
			tag.put("binding", "java.lang.String");
			
			List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
			list.add(geometry);
			list.add(timestamp);
			list.add(etimestamp);
			list.add(duration);
			list.add(numPoints);
			list.add(trackId);
			list.add(userId);
			list.add(user);
			list.add(desc);
			list.add(tag);			
			
			Map<String, List<Map<String, Object>>> attributes = new LinkedHashMap<String, List<Map<String, Object>>>();
			attributes.put("attribute", list);
			
			layer.setAttributes(attributes);

			String input = layer.toJSONString();
			LOGGER.info("GeoServer rest input: " + input);
			
			URL url = new URL(geoserverRestUrl + "/workspaces/geowave/datastores/" + dataStore + "/featuretypes.json");
			
			HttpURLConnection connection =
					(HttpURLConnection) url.openConnection();
			
			String userpass = geoserverUsername + ":" + geoserverPassword;
			@SuppressWarnings("restriction")
			String basicAuth = "Basic " + new String(new sun.misc.BASE64Encoder().encode(userpass.getBytes()));
			connection.setRequestProperty ("Authorization", basicAuth);
			
			connection.setDoOutput(true);
			connection.setRequestMethod("POST");
			connection.setRequestProperty("Content-Type", "application/json");
			connection.setRequestProperty("Accept", "application/json");
			
			OutputStream os = connection.getOutputStream();
			os.write(input.getBytes());
			
			os.flush();
			os.close();
			
			if (connection.getResponseCode() != HttpURLConnection.HTTP_CREATED) {
				LOGGER.error("Failed : HTTP error code : " + connection.getResponseCode());
				throw new RuntimeException("Failed : HTTP error code : "
						+ connection.getResponseCode());
			}

			BufferedReader br = new BufferedReader(new InputStreamReader(
					(connection.getInputStream())));

			String output;
			LOGGER.info("publishDataStore() response code: " + connection.getResponseCode());
			while ((output = br.readLine()) != null) {
				LOGGER.info("Response from GeoServer: " + output);
			}

			connection.disconnect();
		}
		catch (JSONException | IOException e) {
			LOGGER.error(e.getMessage());
		}
	}

	private static BasicAccumuloOperations getOperations(String namespace) throws AccumuloException, AccumuloSecurityException, IOException {
		loadProperties();

		return new BasicAccumuloOperations(zookeeperUrl,
				instanceName,
				geowaveUsername,
				geowavePassword,
				namespace);
	}

	private static AccumuloDataStore getDataStore(String namespace) throws AccumuloException, AccumuloSecurityException, TableNotFoundException, IOException {
		AccumuloDataStore dataStore = null;
		BasicAccumuloOperations operations;
		operations = getOperations(namespace);
		Connector connector = operations.getConnector();

		for (String tableName : connector.tableOperations().list()) {
			if (tableName.startsWith(namespace)
					&& tableName.contains(AbstractAccumuloPersistence.METADATA_TABLE)) {

				IndexStore indexStore = new AccumuloIndexStore(operations);
				AdapterStore adapterStore = new AccumuloAdapterStore(operations);
				Scanner scanner = operations.createScanner(tableName.replace(namespace + "_", ""));

				for (Entry<Key, Value> entry : scanner) {
					if ("INDEX".equals(entry.getKey().getColumnFamily().toString())) {
						indexStore.addIndex(PersistenceUtils.fromBinary(entry.getValue().get(), Index.class));
					}
					else if ("ADAPTER".equals(entry.getKey().getColumnFamily().toString())) {
						adapterStore.addAdapter(PersistenceUtils.fromBinary(entry.getValue().get(), DataAdapter.class));
					}
				}

				dataStore = new AccumuloDataStore(indexStore, adapterStore, operations);
			}
		}
		return dataStore;
	}
	
	private static void loadProperties() throws IOException {
		if (!loaded) {
			Properties prop = new Properties();
			String propFileName = "config.properties";

			InputStream inputStream = Services.class.getClassLoader().getResourceAsStream(propFileName);
			prop.load(inputStream);
			if (inputStream == null) {
				throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
			}

			zookeeperUrl = prop.getProperty("zookeeperUrl");
			instanceName = prop.getProperty("instanceName");
			geowaveUsername = prop.getProperty("geowave_username");
			geowavePassword = prop.getProperty("geowave_password");

			geoserverRestUrl = prop.getProperty("geoserver_rest_url");

			geoserverUsername = prop.getProperty("geoserver_username");
			geoserverPassword = prop.getProperty("geoserver_password");

			loaded = true;
		}
	}

	private static boolean loaded = false;
	private static String zookeeperUrl;
	private static String instanceName;
	private static String geowaveUsername;
	private static String geowavePassword;
	
	private static String geoserverRestUrl;
	
	private static String geoserverUsername;
	private static String geoserverPassword;
	
	private final static Logger LOGGER = Logger.getLogger(Services.class);
}
