package it.nuccioservizi.tailor.as400;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import javax.swing.JTextField;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;

public class AppProperties {

	static final class UpdatePropertyDocumentListener implements DocumentListener {
		private final AppProperties	properties;
		private final Property			property;
		private final JTextField		textField;

		public UpdatePropertyDocumentListener(final AppProperties properties, final Property property, final JTextField textField) {
			this.properties = properties;
			this.property = property;
			this.textField = textField;
		}

		@Override
		public void changedUpdate(final DocumentEvent e) {
			updateProperty();
		}

		@Override
		public void insertUpdate(final DocumentEvent e) {
			updateProperty();
		}

		@Override
		public void removeUpdate(final DocumentEvent e) {
			updateProperty();
		}

		private void updateProperty() {
			properties.set(property, textField.getText());
		}
	}

	public static final String	LOCAL_PROPERTIES_FILE_NAME		= "local.properties";
	private static final String	DEFAULT_PROPERTIES_FILE_NAME	= "/defaults.properties";

	public synchronized static AppProperties load() throws IOException {
		final Properties defaultProperties = new Properties();
		loadPropertiesFromResource(DEFAULT_PROPERTIES_FILE_NAME, defaultProperties);

		Property.setDynamicDefaults(defaultProperties);

		final Properties properties = new Properties(defaultProperties);
		try {
			loadPropertiesFromFile(LOCAL_PROPERTIES_FILE_NAME, properties);
		} catch (final FileNotFoundException e) {
			// Niente proprietà locali.
		}
		return new AppProperties(properties);
	}

	private static void loadPropertiesFromFile(final String propertiesFileName, final Properties properties) throws IOException {
		FileInputStream in = null;
		try {
			in = new FileInputStream(propertiesFileName);
			properties.load(in);
		} finally {
			if (in != null)
				try {
					in.close();
				} catch (final IOException e) {
					// Niente da fare
				}
		}
	}

	private static void loadPropertiesFromResource(final String propertiesResourceName, final Properties properties)
			throws IOException {
		InputStream in = null;
		try {
			in = AppProperties.class.getResourceAsStream(propertiesResourceName);
			properties.load(in);
		} finally {
			if (in != null)
				try {
					in.close();
				} catch (final IOException e) {
					// Niente da fare
				}
		}
	}

	private final Properties	properties;

	private AppProperties(final Properties properties) {
		Property.validate(properties);
		this.properties = properties;
	}

	public synchronized String get(final Property property) {
		return property.get(properties);
	}

	public synchronized boolean isSet(final Property property) {
		return property.isSet(properties);
	}

	public synchronized void save() throws IOException {
		FileOutputStream out = null;
		try {
			out = new FileOutputStream(LOCAL_PROPERTIES_FILE_NAME);
			properties.store(out, "--- Modificare questo file per configurare l'applicazione ---");
		} finally {
			if (out != null)
				out.close();
		}
	}

	public synchronized void set(final Property property, final String value) {
		property.set(properties, value);
	}

}
