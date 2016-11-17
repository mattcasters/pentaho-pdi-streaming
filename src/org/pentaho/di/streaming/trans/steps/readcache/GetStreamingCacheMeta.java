package org.pentaho.di.streaming.trans.steps.readcache;

import java.util.List;

import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.encryption.Encr;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaDate;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.streaming.StreamingService;
import org.pentaho.di.streaming.www.cache.StreamingCacheEntry;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.persist.MetaStoreFactory;
import org.pentaho.metastore.util.PentahoDefaults;
import org.w3c.dom.Node;

@Step(
    id = "GetStreamingCache",
    description = "Reads the current content of a streaming service cache",
    name = "Get Streaming Cache",
    image = "ui/images/TIP.svg",
    categoryDescription = "Input"
    )
public class GetStreamingCacheMeta extends BaseStepMeta implements StepMetaInterface {

  private static final String TAG_SERVICE_NAME = "service_name";
  private static final String TAG_ID_FIELD = "id_field";
  private static final String TAG_TIMESTAMP_FIELD = "timestamp_field";
  private static final String TAG_SERVICE_URL = "service_url";
  private static final String TAG_USERNAME = "username";
  private static final String TAG_PASSWORD = "password";
  
  private String serviceName;
  private String serviceUrl;
  private String username;
  private String password;
  
  private String idField;
  private String timestampField;
  
  public GetStreamingCacheMeta() {
    super();
  }
  
  @Override
  public void getFields(RowMetaInterface inputRowMeta, String name, RowMetaInterface[] info, StepMeta nextStep, 
      VariableSpace space, Repository repository, IMetaStore metaStore) throws KettleStepException {
    
    ValueMetaInterface idValueMeta = new ValueMetaString(idField);
    inputRowMeta.addValueMeta(idValueMeta);
    
    ValueMetaInterface timestampValueMeta = new ValueMetaDate(timestampField);
    inputRowMeta.addValueMeta(timestampValueMeta);
    
    // load service
    //
    try {
      MetaStoreFactory<StreamingService> factory = new MetaStoreFactory<StreamingService>(StreamingService.class, metaStore, PentahoDefaults.NAMESPACE);
      StreamingService service = factory.loadElement(serviceName);
      if (service!=null) {
        StreamingCacheEntry entry = service.getStreamingCache(LogChannel.METADATA, serviceName, username, password, serviceUrl);
        inputRowMeta.addRowMeta(entry.getRowMeta());
      } else {
        throw new KettleException("Unable to find streaming service '"+serviceName+"'");
      }
    } catch(Exception e) {
      throw new KettleStepException("Unable to information from service '"+serviceName+"'", e);
    }
  }

  @Override
  public String getXML() throws KettleException {
    StringBuilder xml = new StringBuilder();
    
    xml.append( XMLHandler.addTagValue( TAG_SERVICE_NAME, serviceName ) );
    xml.append( XMLHandler.addTagValue( TAG_ID_FIELD, idField ) );
    xml.append( XMLHandler.addTagValue( TAG_TIMESTAMP_FIELD, timestampField ) );
    xml.append( XMLHandler.addTagValue( TAG_SERVICE_URL, serviceUrl ) );
    xml.append( XMLHandler.addTagValue( TAG_USERNAME, username) );
    xml.append( XMLHandler.addTagValue( TAG_PASSWORD, Encr.encryptPasswordIfNotUsingVariables(password)) );

    return xml.toString();
  }

  @Override
  public void loadXML( Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore ) throws KettleXMLException {
    try {

      serviceName = XMLHandler.getTagValue( stepnode, TAG_SERVICE_NAME );
      idField = XMLHandler.getTagValue( stepnode, TAG_ID_FIELD );
      timestampField = XMLHandler.getTagValue( stepnode, TAG_TIMESTAMP_FIELD );
      serviceUrl = XMLHandler.getTagValue( stepnode, TAG_SERVICE_URL );
      username = XMLHandler.getTagValue( stepnode, TAG_USERNAME );
      password = Encr.decryptPasswordOptionallyEncrypted(XMLHandler.getTagValue( stepnode, TAG_PASSWORD ));
      
    } catch ( Exception e ) {
      throw new KettleXMLException( "Unable to load execute test step details", e );
    }
  }
  
  @Override
  public void saveRep( Repository rep, IMetaStore metaStore, ObjectId id_transformation, ObjectId id_step ) throws KettleException {
    rep.saveStepAttribute( id_transformation, id_step, TAG_SERVICE_NAME, serviceName );
    rep.saveStepAttribute( id_transformation, id_step, TAG_ID_FIELD, idField );
    rep.saveStepAttribute( id_transformation, id_step, TAG_TIMESTAMP_FIELD, timestampField );
    rep.saveStepAttribute( id_transformation, id_step, TAG_SERVICE_URL, serviceUrl );
    rep.saveStepAttribute( id_transformation, id_step, TAG_USERNAME, username);
    rep.saveStepAttribute( id_transformation, id_step, TAG_PASSWORD, Encr.encryptPasswordIfNotUsingVariables(password));
  }

  @Override
  public void readRep( Repository rep, IMetaStore metaStore, ObjectId id_step, List<DatabaseMeta> databases ) throws KettleException {
    serviceName = rep.getStepAttributeString( id_step, TAG_SERVICE_NAME );
    idField = rep.getStepAttributeString(id_step, TAG_ID_FIELD);
    timestampField = rep.getStepAttributeString(id_step, TAG_TIMESTAMP_FIELD);
    serviceUrl = rep.getStepAttributeString( id_step, TAG_SERVICE_URL );
    username = rep.getStepAttributeString(id_step, TAG_USERNAME);
    password = Encr.decryptPasswordOptionallyEncrypted(rep.getStepAttributeString(id_step, TAG_PASSWORD));
  }


  @Override
  public StepInterface getStep(StepMeta meta, StepDataInterface data, int copy, TransMeta transMeta, Trans trans) {
    return new GetStreamingCache(meta, data, copy, transMeta, trans);
  }

  @Override
  public StepDataInterface getStepData() {
    return new GetStreamingCacheData();
  }
  
  @Override
  public String getDialogClassName() {
    return GetStreamingCacheDialog.class.getName();
  }

  @Override
  public void setDefault() {
    idField = "id";
    timestampField = "timestamp";  
    username = "cluster";  
    password = "cluster";  
    serviceUrl = "http://localhost:8081/kettle/getStreaming";
  }

  public String getServiceName() {
    return serviceName;
  }

  public void setServiceName(String serviceName) {
    this.serviceName = serviceName;
  }

  public String getIdField() {
    return idField;
  }

  public void setIdField(String idField) {
    this.idField = idField;
  }

  public String getTimestampField() {
    return timestampField;
  }

  public void setTimestampField(String timestampField) {
    this.timestampField = timestampField;
  }

  public String getUsername() {
    return username;
  }

  public void setUsername(String username) {
    this.username = username;
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public String getServiceUrl() {
    return serviceUrl;
  }

  public void setServiceUrl(String serviceUrl) {
    this.serviceUrl = serviceUrl;
  } 
}
