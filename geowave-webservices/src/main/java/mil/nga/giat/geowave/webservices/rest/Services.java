package mil.nga.giat.geowave.webservices.rest;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.FactoryConfigurationError;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

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
import mil.nga.giat.geowave.webservices.rest.data.DatastoreEncoder;
import mil.nga.giat.geowave.webservices.rest.data.FeatureTypeEncoder;
import mil.nga.giat.geowave.webservices.rest.data.GeowaveRESTPublisher;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.log4j.Logger;
import org.opengis.feature.simple.SimpleFeatureType;
import org.w3c.dom.CharacterData;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

@Path("/services")
public class Services
{
	@Context
	private static HttpServletRequest request;

	public static void main(String [] args) {
		try {
			loadProperties();

			/**
			 * Get Geowave Namespaces
			 */
			List<String> namespaces = new ArrayList<String>(); 

			Response response = getGeowaveNamespaces();
			
			if (response.getStatus() == Status.OK.getStatusCode()) {
				String xml = response.getEntity().toString();
				
				System.out.println(xml);

				DocumentBuilder db = DocumentBuilderFactory.newInstance().newDocumentBuilder();
				InputSource is = new InputSource();
				is.setCharacterStream(new StringReader(xml));

				Document doc = db.parse(is);
				NodeList nodes = doc.getElementsByTagName("namespaces");

				for (int i = 0; i < nodes.getLength(); i++) {
					Element element = (Element) nodes.item(i);

					NodeList namespace = element.getElementsByTagName("namespace");
					for (int ii = 0; ii < namespace.getLength(); ii++) {
						Element line = (Element) namespace.item(ii);
						Node child = line.getFirstChild();
						if (child instanceof CharacterData) {
							CharacterData cd = (CharacterData) child;
							String ns = cd.getData();
							namespaces.add(ns);

							publishDataStore(ns);
						}
					}
				}
				
				for (String namespace : namespaces) {
					response = getGeowaveLayers(namespace);
					
					if (response.getStatus() == Status.OK.getStatusCode()) {
					xml = response.getEntity().toString();
					db = DocumentBuilderFactory.newInstance().newDocumentBuilder();
					is = new InputSource();
					is.setCharacterStream(new StringReader(xml));

					doc = db.parse(is);
					nodes = doc.getElementsByTagName("namespace");

					for (int i = 0; i < nodes.getLength(); i++) {
						Element element = (Element) nodes.item(i);

						String dataStore = "";
						NodeList names = element.getElementsByTagName("name");
						Element line = (Element) names.item(0);
						Node child = line.getFirstChild();
						if (child instanceof CharacterData) {
							CharacterData cd = (CharacterData) child;
							dataStore = cd.getData();
						}
						
						NodeList layers = element.getElementsByTagName("layer");
						for (int ii = 0; ii < layers.getLength(); ii++) {
							line = (Element) layers.item(ii);
							child = line.getFirstChild();
							if (child instanceof CharacterData) {
								CharacterData cd = (CharacterData) child;
								String layer = cd.getData();
								
								publishLayer(dataStore, layer);
							}
						}
					}
					}
				}
				
			}
		    
//			Response response = getGeowaveNamespaces();
//			JSONObject namespaceInfo = new JSONObject(response.getEntity().toString());
//			
//			List<String> namespaces = new ArrayList<String>(); 
//			LOGGER.debug("GeoWave Namespace Information");
//			for(Iterator<?> iter = namespaceInfo.keys(); iter.hasNext(); ) {
//				String key = iter.next().toString();
//				LOGGER.debug(key + ":" + namespaceInfo.get(key));
//				
//				if (key.equals("namespaces")) {
//					JSONArray jNamespaces = namespaceInfo.getJSONArray(key);
//					for (int index = 0; index < jNamespaces.length(); index++) {
//						String namespace = jNamespaces.getString(index);
//						namespaces.add(namespace);
//						
//						publishDataStore(namespace);
//					}
//				}
//			}
			
//			/**
//			 * Get Geowave Layers
//			 */
//			LOGGER.debug("GeoWave Layer Information");
//			for (String namespace : namespaces) {
//				// issues with raster_test data set
//				if (!"raster_test".equals(namespace)) {
//					response = getGeowaveLayers(namespace);
//					LOGGER.debug(response.getEntity().toString());
//					JSONObject layerInfo = new JSONObject(response.getEntity().toString());					
//					JSONArray layers = layerInfo.getJSONArray("layers");
//					for (int index = 0; index < layers.length(); index++) {
//						String layer = layers.getString(index);						
//						publishLayer(namespace, layer);
//					}					
//				}
//			}
		}
		catch (ParserConfigurationException | FactoryConfigurationError | SAXException | IOException e) {
			LOGGER.error("Exception: " + e.getClass().getName());
			LOGGER.error("Message: " + e.getMessage());
		}
	}

	@GET
	@Produces({MediaType.APPLICATION_XML})
	@Path("/geowaveNamespaces")
	public static Response getGeowaveNamespaces() {

		try {
			DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder docBuilder = docFactory.newDocumentBuilder();
			
			Document document = docBuilder.newDocument();
			Element rootElement = document.createElement("GeowaveNamespaces");
			document.appendChild(rootElement);
			
			Element zkElement = document.createElement("zookeeperUrl");
			zkElement.appendChild(document.createTextNode(zookeeperUrl));
			rootElement.appendChild(zkElement);
			
			Element instElement = document.createElement("instanceName");
			instElement.appendChild(document.createTextNode(instanceName));
			rootElement.appendChild(instElement);
			
			Element userElement = document.createElement("username");
			userElement.appendChild(document.createTextNode(geowaveUsername));
			rootElement.appendChild(userElement);
			
			Element passElement = document.createElement("password");
			passElement.appendChild(document.createTextNode(geowavePassword));
			rootElement.appendChild(passElement);
			
			Element nsListElement = document.createElement("namespaces");
			List<String> namespaces = new ArrayList<String>();

			TableOperations tableOperations = getOperations("").getConnector().tableOperations();
			List<String> allTables = new ArrayList<String>();
			allTables.addAll(tableOperations.list());

			for (String table : allTables) {
				if (table.contains("_GEOWAVE_METADATA")) {
					String namespace = table.substring(0, table.indexOf("_GEOWAVE_METADATA"));
					if (!namespaces.contains(namespace)) {
						namespaces.add(namespace);
						
						Element nsElement = document.createElement("namespace");
						nsElement.appendChild(document.createTextNode(namespace));
						nsListElement.appendChild(nsElement);
					}
				}
			}
			rootElement.appendChild(nsListElement);

			StringWriter writer = new StringWriter();
			
			TransformerFactory transformerFactory = TransformerFactory.newInstance();
			Transformer transformer = transformerFactory.newTransformer();
			DOMSource source = new DOMSource(document);
			StreamResult result = new StreamResult(writer);
			
			transformer.transform(source, result);

			return Response.status(Status.OK).entity(writer.toString()).build();
		}
		catch (AccumuloException | AccumuloSecurityException | IOException | ParserConfigurationException | TransformerException e) {
			LOGGER.error(e.getMessage());
			return Response.status(Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build();
		}
		
	}
	
	@GET
	@Produces({MediaType.APPLICATION_XML})
	@Path("/geowaveLayers/{namespace}")
	public static Response getGeowaveLayers(@PathParam("namespace")String namespace) {
		try {
			DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder docBuilder = docFactory.newDocumentBuilder();
			
			Document document = docBuilder.newDocument();
			Element rootElement = document.createElement("GeowaveLayers");
			document.appendChild(rootElement);
			
			Element nsElement = document.createElement("namespace");
			rootElement.appendChild(nsElement);
			
			Element nameElement = document.createElement("name");
			nameElement.appendChild(document.createTextNode(namespace));
			nsElement.appendChild(nameElement);
			
			Element lsElement = document.createElement("layers");
			nsElement.appendChild(lsElement);
			
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
								
								Element lElement = document.createElement("layer");
								lElement.appendChild(document.createTextNode(simpleFeatureType.getTypeName()));
								lsElement.appendChild(lElement);
							}
						}
					}
				}
			}
			
			StringWriter writer = new StringWriter();
			
			TransformerFactory transformerFactory = TransformerFactory.newInstance();
			Transformer transformer = transformerFactory.newTransformer();
			DOMSource source = new DOMSource(document);
			StreamResult result = new StreamResult(writer);
			
			transformer.transform(source, result);

			return Response.status(200).entity(writer.toString()).build();
		}
		catch (AccumuloException | AccumuloSecurityException | TableNotFoundException | IOException | ParserConfigurationException | TransformerException e) {
			LOGGER.error(e.getMessage());
			return Response.status(Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build();
		}
	}


	@POST
	@Path("/publishDataStore")
	@Consumes(MediaType.APPLICATION_FORM_URLENCODED)
	public static Response publishDataStore(MultivaluedMap<String, String> parameter) {
		String dataStore = null;
		for (String key : parameter.keySet()) {
			if (key.equals("dataStore"))
				dataStore = parameter.getFirst(key);
		}
		boolean flag = false;
		if (dataStore != null)
			flag = publishDataStore(dataStore);
		else
			return Response.status(Status.INTERNAL_SERVER_ERROR).entity("No value entered for Data Store.").build();
		
		if (flag)
			return Response.status(Status.OK).entity("Datastore published.").build();
		else
			return Response.status(Status.INTERNAL_SERVER_ERROR).entity("Error occurred.").build();
	}
	
	@POST
	@Path("/publishLayer")
	@Consumes(MediaType.APPLICATION_FORM_URLENCODED)
	public static Response publishLayer(MultivaluedMap<String, String> parameter) {
		String dataStore = null;
		String layer = null;
		for (String key : parameter.keySet()) {
			if (key.equals("dataStore"))
				dataStore = parameter.getFirst(key);
			else if (key.equals("layer"))
				layer = parameter.getFirst(key);
		}
		boolean flag = false;
		if (dataStore != null && layer != null)
			flag = publishLayer(dataStore, layer);
		else
			return Response.status(Status.INTERNAL_SERVER_ERROR).entity("No value entered for Data Store and/or Layer.").build();
		
		if (flag)
			return Response.status(Status.OK).entity("Layer published.").build();
		else
			return Response.status(Status.INTERNAL_SERVER_ERROR).entity("Error occurred.").build();
	}
	
	public static boolean publishDataStore(String name) {
		return publishDataStore(name, name);
	}
	
	public static boolean publishDataStore(String name, String namespace) {
		boolean flag = false;
		try {
			DatastoreEncoder encoder = new DatastoreEncoder();
			encoder.setName(name);
			encoder.setType("GeoWave DataStore");
			encoder.setEnabled(true);

			loadProperties();
			
			Map<String, String> cp = new HashMap<String, String>();
			cp.put("ZookeeperServers", zookeeperUrl);
			cp.put("Password", geowavePassword);
			cp.put("Namespace", namespace);
			cp.put("UserName", geowaveUsername);
			cp.put("InstanceName", instanceName);
			
			encoder.setConnectionParameters(cp);
			
			GeowaveRESTPublisher publisher = new GeowaveRESTPublisher(geoserverUrl, geoserverUsername, geoserverPassword);
			
			if (publisher.datastoreExist(geoserverWorkspace, name)) {
				LOGGER.info("Datastore: " + name + " already exists.");
			}
			else {
				flag = publisher.createDatastore(geoserverWorkspace, encoder);
			}
		}
		catch (IOException e) {
			LOGGER.error("Exception: " + e.getClass().getName());
			LOGGER.error("Message: " + e.getMessage());
		}
		return flag;
	}

	public static boolean publishLayer(String dataStore, String layerName) {
		boolean flag = false;
		try {
			FeatureTypeEncoder layer = new FeatureTypeEncoder();

			layer.setName(layerName);
			layer.setNativeName(layerName);
			layer.setTitle(layerName);
			
			Map<String, Collection<String>> keywords = new LinkedHashMap<String, Collection<String>>();
			Collection<String> temp = new ArrayList<String>();
			temp.add(layerName);
			keywords.put("string", temp);
			layer.setKeywords(keywords);
			
			LOGGER.info("GeoServer rest input: " + layer.toString());
			
			loadProperties();
			
			GeowaveRESTPublisher publisher = new GeowaveRESTPublisher(geoserverUrl, geoserverUsername, geoserverPassword);
			
			if (publisher.layerExist(layerName)) {
				LOGGER.info("Layer: " + layerName + " already exists.");
			}
			else {
				flag = publisher.publishLayer(geoserverWorkspace, dataStore, layer);
			}
		}
		catch (IOException e) {
			LOGGER.error(e.getMessage());
		}
		return flag;
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

			geoserverUrl = prop.getProperty("geoserver_url");
			geoserverUsername = prop.getProperty("geoserver_username");
			geoserverPassword = prop.getProperty("geoserver_password");
			
			geoserverWorkspace = prop.getProperty("geoserver_workspace");

			loaded = true;
		}
	}

	private static boolean loaded = false;
	private static String zookeeperUrl;
	private static String instanceName;
	private static String geowaveUsername;
	private static String geowavePassword;
	
	private static String geoserverUrl;
	
	private static String geoserverUsername;
	private static String geoserverPassword;
	
	private static String geoserverWorkspace;
	
	private final static Logger LOGGER = Logger.getLogger(Services.class);
}
