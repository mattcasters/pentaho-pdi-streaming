package org.pentaho.di.streaming.trans.steps.readcache;

import org.pentaho.di.cluster.SlaveServer;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.streaming.StreamingService;
import org.pentaho.di.streaming.www.cache.StreamingCacheEntry;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.persist.MetaStoreFactory;

public class GetStreamingCacheData extends BaseStepData implements StepDataInterface {

  public RowMetaInterface outputRowMeta;
  public MetaStoreFactory<StreamingService> factory;
  public StreamingService service;
  public StreamingCacheEntry streamingCache;
  public int rowIndex;
  public IMetaStore store;
  public SlaveServer slaveServer;
  
  public GetStreamingCacheData() {
  }
  
}
