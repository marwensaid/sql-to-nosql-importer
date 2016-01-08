package orange.save.export.sql.model;
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.sql.DataSource;
import java.io.IOException;
import java.util.*;

/**
 * <p>
 * Mapping for data-config.xml
 * </p>
 * <p/>
 * <p>
 * Refer to <a
 * href="http://wiki.apache.org/solr/DataImportHandler">http://wiki.apache.org/solr/DataImportHandler</a>
 * for more details.
 * </p>
 * <p/>
 * <b>This API is experimental and subject to change</b>
 *
 * @since solr 1.3
 */
public class DataConfig {
 

  public Document document;

  public List<Map<String, String >> functions = new ArrayList<Map<String ,String>>();

  public Script script;

  public Map<String, Properties> dataSources = new HashMap<String, Properties>();



  boolean isMultiThreaded = false;

  public static class Document {
    // TODO - remove from here and add it to entity
    public String deleteQuery;

    public List<Entity> entities = new ArrayList<Entity>();

    public String onImportStart, onImportEnd;

    public Document() {
    }

    public Document(Element element) throws IOException {
      this.deleteQuery = getStringAttribute(element, "deleteQuery", null);
      this.onImportStart = getStringAttribute(element, "onImportStart", null);
      this.onImportEnd = getStringAttribute(element, "onImportEnd", null);
      List<Element> l = getChildNodes(element, "entity");
      for (Element e : l)
        entities.add(new Entity(e));
    }
  }

  public static class Entity {
    public String name;

    public String pk;

    public String pkMappingFromSchema;

    public String dataSource;

    public Map<String, String> allAttributes;

    public String proc;

    public String docRoot;

    public boolean isDocRoot = false;

    public List<Field> fields = new ArrayList<Field>();

    public List<Map<String, String>> allFieldsList = new ArrayList<Map<String, String>>();

    public List<Entity> entities;

    public Entity parentEntity;


    public DataSource dataSrc;

    public Map<String, List<Field>> colNameVsField = new HashMap<String, List<Field>>();

    public Entity() {
    }

    public Entity(Element element) throws IOException {
      name = getStringAttribute(element, NAME, null);
    
      if(name.indexOf(".") != -1){
        throw new IOException();
      }      
 
      pk = getStringAttribute(element, "pk", null);
      docRoot = getStringAttribute(element, ROOT_ENTITY, null);
      proc = getStringAttribute(element, PROCESSOR, null);
      dataSource = getStringAttribute(element, DATA_SRC, null);
      allAttributes = getAllAttributes(element);
      List<Element> elementList = getChildNodes(element, "field");
      for (Element element1 : elementList)  {
        Field field = new Field(element1);
        fields.add(field);
        List<Field> fieldList = colNameVsField.get(field.column);
        if(fieldList == null) fieldList = new ArrayList<Field>();
        boolean alreadyFound = false;
        for (Field field1 : fieldList) {
          if(field1.getName().equals(field.getName())) {
            alreadyFound = true;
            break;
          }
        }
        if(!alreadyFound) fieldList.add(field);
        colNameVsField.put(field.column, fieldList);
      }
      elementList = getChildNodes(element, "entity");
      if (!elementList.isEmpty())
        entities = new ArrayList<Entity>();
      for (Element element1 : elementList)
        entities.add(new Entity(element1));

    }

  
    public String getPk(){
      return pk == null ? pkMappingFromSchema : pk;
    }

    public String getSchemaPk(){
      return pkMappingFromSchema != null ? pkMappingFromSchema : pk;
    }
  }

  public static class Script {
    public String language;

    public String text;

    public Script() {
    }

    public Script(Element element) {
      this.language = getStringAttribute(element, "language", "JavaScript");
      StringBuilder buffer = new StringBuilder();
      String script = getTxt(element, buffer);
      if (script != null)
        this.text = script.trim();
    }
  }

  public static class Field {

    public String column;

    public String name;

    public String defaultValue;

    public Float boost = 1.0f;

    public boolean toWrite = true;

    public boolean multiValued = false;

    boolean dynamicName;


    public Map<String, String> allAttributes = new HashMap<String, String>() {
      /**
		 * 
		 */
		private static final long serialVersionUID = 1L;

	@Override
      public String put(String key, String value) {
        if (super.containsKey(key))
          return super.get(key);
        return super.put(key, value);
      }
    };

    public Field() {
    }

    public Field(Element element) throws IOException {
      this.name = getStringAttribute(element, "name", null);
      this.column = getStringAttribute(element, "column", null);
      if (column == null) {
        throw new IOException();
      }
      this.boost = Float.parseFloat(getStringAttribute(element, "boost", "1.0f"));
      allAttributes.putAll(getAllAttributes(element));
    }

    public String getName() {
      return name == null ? column : name;
    }

    public Entity entity;

  }

  public void readFromXml(Element element1) throws IOException {
    List<Element> elementList = getChildNodes(element1, "document");
    if (elementList.isEmpty()) {
      throw new IOException();
    }
    document = new Document(elementList.get(0));

    elementList = getChildNodes(element1, SCRIPT);
    if (!elementList.isEmpty()) {
      script = new Script(elementList.get(0));
    }

    // Add the provided evaluators
    elementList = getChildNodes(element1, FUNCTION);
    if (!elementList.isEmpty()) {
      for (Element element : elementList) {
        String func = getStringAttribute(element, NAME, null);
        String clz = getStringAttribute(element, CLASS, null);
        if (func == null || clz == null){
          throw new IOException();
        } else {
          functions.add(getAllAttributes(element));
        }
      }
    }
    elementList = getChildNodes(element1, DATA_SRC);
    if (!elementList.isEmpty()) {
      for (Element element : elementList) {
        Properties properties = new Properties();
        HashMap<String, String> attrs = getAllAttributes(element);
        for (Map.Entry<String, String> entry : attrs.entrySet()) {
          properties.setProperty(entry.getKey(), entry.getValue());
        }
        dataSources.put(properties.getProperty("name"), properties);
      }
    }
    if(dataSources.get(null) == null){
      for (Properties properties : dataSources.values()) {
        dataSources.put(null,properties);
        break;        
      } 
    }
  }

  private static String getStringAttribute(Element element, String name, String def) {
    String elementAttribute = element.getAttribute(name);
    if (elementAttribute == null || "".equals(elementAttribute.trim()))
      elementAttribute = def;
    return elementAttribute;
  }

  private static HashMap<String, String> getAllAttributes(Element e) {
    HashMap<String, String> map = new HashMap<String, String>();
    NamedNodeMap nnm = e.getAttributes();
    for (int i = 0; i < nnm.getLength(); i++) {
      map.put(nnm.item(i).getNodeName(), nnm.item(i).getNodeValue());
    }
    return map;
  }

  public static String getTxt(Node node, StringBuilder stringBuilder) {
    if (node.getNodeType() != Node.CDATA_SECTION_NODE) {
      NodeList nodeList = node.getChildNodes();
      for (int i = 0; i < nodeList.getLength(); i++) {
        Node child = nodeList.item(i);
        short childType = child.getNodeType();
        if (childType != Node.COMMENT_NODE
                && childType != Node.PROCESSING_INSTRUCTION_NODE) {
          getTxt(child, stringBuilder);
        }
      }
    } else {
      stringBuilder.append(node.getNodeValue());
    }

    return stringBuilder.toString();
  }

  public static List<Element> getChildNodes(Element element, String byName) {
    List<Element> result = new ArrayList<Element>();
    NodeList nodeList = element.getChildNodes();
    for (int i = 0; i < nodeList.getLength(); i++) {
      if (element.equals(nodeList.item(i).getParentNode())
              && byName.equals(nodeList.item(i).getNodeName()))
        result.add((Element) nodeList.item(i));
    }
    return result;
  }



  public static final String SCRIPT = "script";

  public static final String NAME = "name";

  public static final String PROCESSOR = "processor";

  /**
   * @deprecated use IMPORTER_NS_SHORT instead
   */
  @Deprecated
  public static final String IMPORTER_NS = "dataimporter";

  public static final String IMPORTER_NS_SHORT = "dih";

  public static final String ROOT_ENTITY = "rootEntity";

  public static final String FUNCTION = "function";

  public static final String CLASS = "class";

  public static final String DATA_SRC = "dataSource";

}