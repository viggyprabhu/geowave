package mil.nga.giat.geowave.webservices.rest.data;

import java.io.StringWriter;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.log4j.Logger;
import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class DatastoreEncoder
{
	private final static Logger LOGGER = Logger.getLogger(DatastoreEncoder.class);
	
	private static final String DATASTORE = "dataStore";
	private static final String NAME = "name";
	private static final String TYPE = "type";
	private static final String ENABLED = "enabled";
	private static final String CONNECTION_PARAMETERS = "connectionParameters";
	private static final String ENTRY = "entry";
	private static final String KEY = "key";
	private static final String DEFAULT = "__default";

	private String name;
	private String type;
	private boolean enabled;
	private Map<String, String> connectionParameters;
	private boolean _default;

	public DatastoreEncoder() {}

	@Override
	public String toString() {
		String xml = null;
		try {
			DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder docBuilder = docFactory.newDocumentBuilder();
			 
			// root element
			Document document = docBuilder.newDocument();
			Element rootElement = document.createElement(DATASTORE);
			document.appendChild(rootElement);
			
			// name element
			Element nameElement = document.createElement(NAME);
			nameElement.appendChild(document.createTextNode(getName()));
			rootElement.appendChild(nameElement);

			// type element
			Element typeElement = document.createElement(TYPE);
			typeElement.appendChild(document.createTextNode(getType()));
			rootElement.appendChild(typeElement);

			// enabled element
			Element enabledElement = document.createElement(ENABLED);
			enabledElement.appendChild(
					document.createTextNode(Boolean.toString(isEnabled())));
			rootElement.appendChild(enabledElement);
			
			// connection parameters element
			Element connectionElement = document.createElement(CONNECTION_PARAMETERS);
			
			for (String key : connectionParameters.keySet()) {
				// entry element
				Element entryElement = document.createElement(ENTRY);
				// set attribute to entry element
				Attr attribute = document.createAttribute(KEY);
				attribute.setValue(key);
				entryElement.setAttributeNode(attribute);
				
				entryElement.appendChild(document.createTextNode(connectionParameters.get(key)));
				connectionElement.appendChild(entryElement);
			}
			rootElement.appendChild(connectionElement);
			
			// _default parameters element
			Element defaultElement = document.createElement(DEFAULT);
			defaultElement.appendChild(document.createTextNode(Boolean.toString(is_default())));
			rootElement.appendChild(defaultElement);
			
			TransformerFactory tf = TransformerFactory.newInstance();
			Transformer transformer = tf.newTransformer();
			transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
			StringWriter writer = new StringWriter();
			transformer.transform(new DOMSource(document), new StreamResult(writer));
			xml = writer.getBuffer().toString().replaceAll("\n|\r", "");
		}
		catch (ParserConfigurationException | TransformerException e) {
			LOGGER.error(e.getMessage());
		}
		return xml;
	}

	public String getName() {
		return name;
	}

	public void setName(
			String name ) {
		this.name = name;
	}

	public String getType() {
		return type;
	}

	public void setType(
			String type ) {
		this.type = type;
	}

	public boolean isEnabled() {
		return enabled;
	}

	public void setEnabled(
			boolean enabled ) {
		this.enabled = enabled;
	}

	public Map<String, String> getConnectionParameters() {
		return connectionParameters;
	}

	public void setConnectionParameters(
			Map<String, String> connectionParameters ) {
		this.connectionParameters = connectionParameters;
	}

	public boolean is_default() {
		return _default;
	}

	public void set_default(
			boolean _default ) {
		this._default = _default;
	}
}
