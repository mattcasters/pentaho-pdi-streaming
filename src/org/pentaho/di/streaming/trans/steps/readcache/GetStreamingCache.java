package org.pentaho.di.streaming.trans.steps.readcache;

import java.util.Date;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.metastore.MetaStoreConst;
import org.pentaho.di.streaming.StreamingService;
import org.pentaho.di.streaming.www.cache.StreamingTimedNumberedRow;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.metastore.persist.MetaStoreFactory;
import org.pentaho.metastore.util.PentahoDefaults;

public class GetStreamingCache extends BaseStep implements StepInterface {

  public GetStreamingCache(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta, Trans trans) {
    super(stepMeta, stepDataInterface, copyNr, transMeta, trans);    
  }
  
  @Override
  public boolean init(StepMetaInterface smi, StepDataInterface sdi) {
    GetStreamingCacheData data = (GetStreamingCacheData) sdi;
    
    data.rowIndex = 0;
    
    return super.init(smi, sdi);
  }
  
  @Override
  public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException {
    GetStreamingCacheMeta meta = (GetStreamingCacheMeta) smi;
    GetStreamingCacheData data = (GetStreamingCacheData) sdi;

    if (first) {
      first = false;

      try {
        data.store = getAMetaStore();
      
        String serviceName = environmentSubstitute(meta.getServiceName());
        
        data.factory = new MetaStoreFactory<StreamingService>(StreamingService.class, data.store, PentahoDefaults.NAMESPACE);
        data.service = data.factory.loadElement(serviceName);
        
        try {
          String baseUrl = environmentSubstitute(meta.getServiceUrl());
          String username = environmentSubstitute(meta.getUsername());
          String password = environmentSubstitute(meta.getPassword());
          
          data.streamingCache = data.service.getStreamingCache(log, serviceName, username, password, baseUrl);
          
          log.logBasic("Found "+data.streamingCache.getRowData().size()+" rows in the streaming cache");
        } catch(Exception e) {
          log.logError("Unable to read cache data from the streaming service '"+serviceName+"'", e);
          setErrors(1);
          setOutputDone();
          stopAll();
          return false;
        }
      } catch(Exception e) {
        log.logError("Unable to load information from the metastore", e);
        setErrors(1);
        setOutputDone();
        stopAll();
        return false;
      }
      
      data.outputRowMeta = new RowMeta();
      
      // TODO : Does a second call to service, think of cache.
      //
      meta.getFields(data.outputRowMeta, getStepname(), null, null, this, repository, data.store);
    }
    
    log.logBasic("Streaming row "+(data.rowIndex+1)+"/"+data.streamingCache.getRowData().size());
    
    if (data.rowIndex<data.streamingCache.getRowData().size()) {
      RowMetaInterface stnRowMeta = data.streamingCache.getRowMeta();
      StreamingTimedNumberedRow stnRow = data.streamingCache.getRowData().get(data.rowIndex);
      Object[] stnCacheRow = stnRow.getRow();
      
      data.rowIndex++;
      
      Object[] row = RowDataUtil.allocateRowData(data.outputRowMeta.size());
      int index = 0;
      row[index++] = Long.valueOf(stnRow.getId());
      row[index++] = new Date(stnRow.getTime());
      for (int i=0;i<stnRowMeta.size();i++) {
        row[index++] = stnCacheRow[i];
      }
      
      putRow(data.outputRowMeta, row);
      
      return true;
    } else {
      setOutputDone();
      return false;
    }
  }

  private IMetaStore getAMetaStore() throws MetaStoreException {
    IMetaStore store = metaStore;
    // during exec of data service, metaStore is not passed down
    //
    if (store==null) {
      store = getTrans().getMetaStore();
    }
    if (store==null) {
      store = getTransMeta().getMetaStore();
    }
    if (store==null && getTrans().getParentTrans()!=null) {
      store = getTrans().getParentTrans().getMetaStore();
    }
    if (store==null) {
      log.logError("Unable to find the metastore, locating it ourselves...");
      if (repository!=null) {
        store = repository.getMetaStore();
      } else {
        store = MetaStoreConst.openLocalPentahoMetaStore();
      }
    }
    return store;
  }
}
