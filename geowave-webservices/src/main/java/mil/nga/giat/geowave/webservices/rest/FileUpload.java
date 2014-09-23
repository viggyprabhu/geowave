package mil.nga.giat.geowave.webservices.rest;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.FileItemFactory;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.apache.log4j.Logger;

@Path("/files")
public class FileUpload {

	private final static Logger LOGGER = Logger.getLogger(FileUpload.class);	
	private final static String TMP_DIR = "geowave-upload-files";
	private final static String STYLE_NAME = "styleName";
	
	private String styleName;
	private File file;

	@POST
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	@Produces("text/plain")
	@Path("/ingestFile")
	public Response ingestFile(@Context HttpServletRequest request) {
		Response response = null;
		try {
			upload(request);
		}
		catch (Exception e) {
			LOGGER.error(e.getMessage());
		} 
		finally {
			if (file != null) {
				// add ingest logic here
			}
		}
		return response;
	}
	
	@POST
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	@Produces("text/plain")
	@Path("/uploadStyle")
	public Response uploadStyle(@Context HttpServletRequest request) {
		Response response = null;
		try {
			upload(request);
		}
		catch (Exception e) {
			LOGGER.error(e.getMessage());
		} 
		finally {
			if (styleName != null && file != null) {
				Services.publishStyle(styleName, file);
				response = Response.status(Status.CREATED).entity("Style published.").build();
			}

			try {
					Files.delete(file.toPath());
			} catch (IOException e) {
				LOGGER.warn(e);
			}
		}

		if (response == null)
			response = Response.status(Status.INTERNAL_SERVER_ERROR).entity("Error occurred.").build();
		return response;
	}
	
	@POST
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	@Produces("text/plain")
	@Path("/updateStyle")
	public Response updateStyle(@Context HttpServletRequest request) {
		Response response = null;
		try {
			upload(request);
		}
		catch (Exception e) {
			LOGGER.error(e);
		}
		finally {
			if (styleName != null && file != null) {
					Services.updateStyle(styleName, file);
					response = Response.status(Status.CREATED).entity("Style updated.").build();
			}

			try {
				Files.delete(file.toPath());
			} catch (IOException e) {
				LOGGER.warn(e);
			}
		}

		if (response == null)
			response = Response.status(Status.INTERNAL_SERVER_ERROR).entity("Error occurred.").build();
		return response;
	}
	
	private void upload(HttpServletRequest request) throws Exception {
		// create temporary directory
		java.nio.file.Path tempDir = Files.createTempDirectory(TMP_DIR);

		//checks whether there is a file upload request or not
		if (ServletFileUpload.isMultipartContent(request)) {
			FileItemFactory factory = new DiskFileItemFactory();
			ServletFileUpload fileUpload = new ServletFileUpload(factory);

			for(Object obj : fileUpload.parseRequest(request)) {
				if (obj instanceof FileItem) {
					FileItem item = (FileItem)obj;

					// check if it represents an uploaded file
					if (!item.isFormField()) {
						File file = new File(tempDir + File.separator + item.getName());
						item.write(file);
						this.file = file;
					}
					else {
						if (STYLE_NAME.equals(item.getFieldName()))
							styleName = item.getString();
					}
				}
			}
		}
	}
}
