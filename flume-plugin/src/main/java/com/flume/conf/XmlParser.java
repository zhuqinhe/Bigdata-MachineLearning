/**
 * 
 */
package com.flume.conf;

import java.io.InputStream;
import java.io.StringWriter;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zhuqinhe
 */
public class XmlParser<T> {
	
	private final static Logger logger = LoggerFactory.getLogger(XmlParser.class);
	
	/**
	 * Object to XML
	 * @param cls
	 * @param obj
	 * @return
	 */
	public static <T> String marshaller(Class<T> cls,T obj){
		try {
			JAXBContext context = JAXBContext.newInstance(cls);
			Marshaller marshaller = context.createMarshaller();
			marshaller.setProperty(Marshaller.JAXB_ENCODING, "UTF-8");
			marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
			
			StringWriter sw = new StringWriter();			
			marshaller.marshal(obj, sw);			
			return sw.toString();
		} catch (JAXBException e) {			
			logger.error(e.getMessage(),e);
		}		
		return null;
	}
	
	
	/**
	 * XML to Object
	 * @param cls
	 * @param is
	 * @return
	 */
	public static <T> T unmarshaller(Class<T> cls,InputStream is){
		try {
			JAXBContext context = JAXBContext.newInstance(cls);
			Unmarshaller unmarshaller = context.createUnmarshaller();
			
			T obj = (T)unmarshaller.unmarshal(is);
			return obj;			
		} catch (JAXBException e) {			
			logger.error(e.getMessage(),e);
		} 		
		return null;
	}
	
}
	

