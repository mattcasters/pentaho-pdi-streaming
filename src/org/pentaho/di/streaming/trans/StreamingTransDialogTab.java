/*!
* Copyright 2010 - 2013 Pentaho Corporation.  All rights reserved.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*
*/

package org.pentaho.di.streaming.trans;

import java.util.Arrays;
import java.util.List;

import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.streaming.StreamingService;
import org.pentaho.di.streaming.util.StreamingConst;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.ui.core.PropsUI;
import org.pentaho.di.ui.core.dialog.EnterStringDialog;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.di.ui.trans.dialog.TransDialogPlugin;
import org.pentaho.di.ui.trans.dialog.TransDialogPluginInterface;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.metastore.persist.MetaStoreFactory;
import org.pentaho.metastore.util.PentahoDefaults;

@TransDialogPlugin(
  id = "StreamingTransDialogTab",
  name = "Streaming transformation dialog tab plugin",
  description = "This plugin makes sure there's an extra 'Streaming' tab in the transformation settings dialog",
  i18nPackageName = "org.pentaho.di.streaming.trans" )
public class StreamingTransDialogTab implements TransDialogPluginInterface {

  private static Class<?> PKG = StreamingTransDialogTab.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$

  private CTabItem wStreamingServiceTab;
  private CCombo wServiceName;
  private CCombo wServiceStep;
  private TextVar wServiceCacheDuration;
  private TextVar wServiceCacheSize;

  private Button wPreloadService;

  private Button wClearOnStart;

  private Button wReplaceAtTransEnd;

  private CCombo wLogLevel;
  
  @Override
  public void addTab( final TransMeta transMeta, final Shell shell, final CTabFolder wTabFolder ) {

    transMeta.setRepository( Spoon.getInstance().getRepository() );
    transMeta.setMetaStore( Spoon.getInstance().getMetaStore() );

    PropsUI props = PropsUI.getInstance();
    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    wStreamingServiceTab = new CTabItem( wTabFolder, SWT.NONE );
    wStreamingServiceTab.setText( BaseMessages.getString( PKG, "TransDialog.StreamingTab.Label" ) );

    Composite wStreamingServiceComp = new Composite( wTabFolder, SWT.NONE );
    props.setLook( wStreamingServiceComp );

    FormLayout streamingServiceLayout = new FormLayout();
    streamingServiceLayout.marginWidth = Const.FORM_MARGIN;
    streamingServiceLayout.marginHeight = Const.FORM_MARGIN;
    wStreamingServiceComp.setLayout( streamingServiceLayout );

    // 
    // Service name
    //
    Label wlServiceName = new Label( wStreamingServiceComp, SWT.LEFT );
    wlServiceName.setText( BaseMessages.getString( PKG, "TransDialog.ServiceName.Label" ) );
    wlServiceName.setToolTipText( BaseMessages.getString( PKG, "TransDialog.ServiceName.Tooltip" ) );
    props.setLook( wlServiceName );
    FormData fdlServiceName = new FormData();
    fdlServiceName.left = new FormAttachment( 0, 0 );
    fdlServiceName.right = new FormAttachment( middle, -margin );
    fdlServiceName.top = new FormAttachment( 0, 0 );
    wlServiceName.setLayoutData( fdlServiceName );
    wServiceName = new CCombo( wStreamingServiceComp, SWT.LEFT | SWT.BORDER | SWT.SINGLE );
    wServiceName.setToolTipText( BaseMessages.getString( PKG, "TransDialog.ServiceName.Tooltip" ) );
    props.setLook( wServiceName );
    FormData fdServiceName = new FormData();
    fdServiceName.left = new FormAttachment( middle, 0 );
    fdServiceName.right = new FormAttachment( 100, 0 );
    fdServiceName.top = new FormAttachment( 0, 0 );
    wServiceName.setLayoutData( fdServiceName );
    wServiceName.setEditable( false );
    wServiceName.setItems( getServiceElementNames( shell, transMeta.getMetaStore() ) );
    Control lastControl = wServiceName;

    // 
    // Service step
    //
    Label wlServiceStep = new Label( wStreamingServiceComp, SWT.LEFT );
    wlServiceStep.setText( BaseMessages.getString( PKG, "TransDialog.ServiceStep.Label" ) );
    wlServiceStep.setToolTipText( BaseMessages.getString( PKG, "TransDialog.ServiceStep.Tooltip" ) );
    props.setLook( wlServiceStep );
    FormData fdlServiceStep = new FormData();
    fdlServiceStep.left = new FormAttachment( 0, 0 );
    fdlServiceStep.right = new FormAttachment( middle, -margin );
    fdlServiceStep.top = new FormAttachment( lastControl, margin );
    wlServiceStep.setLayoutData( fdlServiceStep );
    wServiceStep = new CCombo( wStreamingServiceComp, SWT.LEFT | SWT.BORDER | SWT.SINGLE );
    wServiceStep.setToolTipText( BaseMessages.getString( PKG, "TransDialog.DataServiceStep.Tooltip" ) );
    props.setLook( wServiceStep );
    FormData fdServiceStep = new FormData();
    fdServiceStep.left = new FormAttachment( middle, 0 );
    fdServiceStep.right = new FormAttachment( 100, 0 );
    fdServiceStep.top = new FormAttachment( lastControl, margin );
    wServiceStep.setLayoutData( fdServiceStep );
    String[] stepnames = transMeta.getStepNames();
    Arrays.sort( stepnames );
    wServiceStep.setItems( stepnames );
    lastControl = wServiceStep;

    // 
    // Cache duration
    //
    Label wlServiceCacheDuration = new Label( wStreamingServiceComp, SWT.LEFT );
    wlServiceCacheDuration.setText( BaseMessages.getString( PKG, "TransDialog.ServiceCacheDuration.Label" ) );
    wlServiceCacheDuration.setToolTipText( BaseMessages.getString( PKG, "TransDialog.ServiceCacheDuration.Tooltip" ) );
    props.setLook( wlServiceCacheDuration );
    FormData fdlServiceCacheDuration = new FormData();
    fdlServiceCacheDuration.left = new FormAttachment( 0, 0 );
    fdlServiceCacheDuration.right = new FormAttachment( middle, -margin );
    fdlServiceCacheDuration.top = new FormAttachment( lastControl, margin );
    wlServiceCacheDuration.setLayoutData( fdlServiceCacheDuration );
    wServiceCacheDuration = new TextVar( transMeta, wStreamingServiceComp, SWT.LEFT | SWT.BORDER | SWT.SINGLE );
    wServiceCacheDuration.setToolTipText( BaseMessages.getString( PKG, "TransDialog.ServiceCacheMethod.Tooltip" ) );
    props.setLook( wServiceCacheDuration );
    FormData fdServiceCacheDuration = new FormData();
    fdServiceCacheDuration.left = new FormAttachment( middle, 0 );
    fdServiceCacheDuration.right = new FormAttachment( 100, 0 );
    fdServiceCacheDuration.top = new FormAttachment( lastControl, margin );
    wServiceCacheDuration.setLayoutData( fdServiceCacheDuration );
    lastControl = wServiceCacheDuration;

    // 
    // Cache size
    //
    Label wlServiceCacheSize = new Label( wStreamingServiceComp, SWT.LEFT );
    wlServiceCacheSize.setText( BaseMessages.getString( PKG, "TransDialog.ServiceCacheSize.Label" ) );
    wlServiceCacheSize.setToolTipText( BaseMessages.getString( PKG, "TransDialog.ServiceCacheSize.Tooltip" ) );
    props.setLook( wlServiceCacheSize );
    FormData fdlServiceCacheSize = new FormData();
    fdlServiceCacheSize.left = new FormAttachment( 0, 0 );
    fdlServiceCacheSize.right = new FormAttachment( middle, -margin );
    fdlServiceCacheSize.top = new FormAttachment( lastControl, margin );
    wlServiceCacheSize.setLayoutData( fdlServiceCacheSize );
    wServiceCacheSize = new TextVar( transMeta, wStreamingServiceComp, SWT.LEFT | SWT.BORDER | SWT.SINGLE );
    wServiceCacheSize.setToolTipText( BaseMessages.getString( PKG, "TransDialog.ServiceCacheSize.Tooltip" ) );
    props.setLook( wServiceCacheSize );
    FormData fdServiceCacheSize = new FormData();
    fdServiceCacheSize.left = new FormAttachment( middle, 0 );
    fdServiceCacheSize.right = new FormAttachment( 100, 0 );
    fdServiceCacheSize.top = new FormAttachment( lastControl, margin );
    wServiceCacheSize.setLayoutData( fdServiceCacheSize );
    lastControl = wServiceCacheSize;

    // 
    // Pre-load service?
    //
    Label wlPreloadService = new Label( wStreamingServiceComp, SWT.LEFT );
    wlPreloadService.setText( BaseMessages.getString( PKG, "TransDialog.PreloadService.Label" ) );
    wlPreloadService.setToolTipText( BaseMessages.getString( PKG, "TransDialog.PreloadService.Tooltip" ) );
    props.setLook( wlPreloadService );
    FormData fdlPreloadService = new FormData();
    fdlPreloadService.left = new FormAttachment( 0, 0 );
    fdlPreloadService.right = new FormAttachment( middle, -margin );
    fdlPreloadService.top = new FormAttachment( lastControl, margin );
    wlPreloadService.setLayoutData( fdlPreloadService );
    wPreloadService = new Button( wStreamingServiceComp, SWT.CHECK );
    wPreloadService.setToolTipText( BaseMessages.getString( PKG, "TransDialog.PreloadService.Tooltip" ) );
    props.setLook( wPreloadService );
    FormData fdPreloadService = new FormData();
    fdPreloadService.left = new FormAttachment( middle, 0 );
    fdPreloadService.right = new FormAttachment( 100, 0 );
    fdPreloadService.top = new FormAttachment( lastControl, margin );
    wPreloadService.setLayoutData( fdPreloadService );
    lastControl = wPreloadService;

    // 
    // Clear cache on start?
    //
    Label wlClearOnStart = new Label( wStreamingServiceComp, SWT.LEFT );
    wlClearOnStart.setText( BaseMessages.getString( PKG, "TransDialog.ClearOnStart.Label" ) );
    wlClearOnStart.setToolTipText( BaseMessages.getString( PKG, "TransDialog.ClearOnStart.Tooltip" ) );
    props.setLook( wlClearOnStart );
    FormData fdlClearOnStart = new FormData();
    fdlClearOnStart.left = new FormAttachment( 0, 0 );
    fdlClearOnStart.right = new FormAttachment( middle, -margin );
    fdlClearOnStart.top = new FormAttachment( lastControl, margin );
    wlClearOnStart.setLayoutData( fdlClearOnStart );
    wClearOnStart = new Button( wStreamingServiceComp, SWT.CHECK );
    wClearOnStart.setToolTipText( BaseMessages.getString( PKG, "TransDialog.ClearOnStart.Tooltip" ) );
    props.setLook( wClearOnStart );
    FormData fdClearOnStart = new FormData();
    fdClearOnStart.left = new FormAttachment( middle, 0 );
    fdClearOnStart.right = new FormAttachment( 100, 0 );
    fdClearOnStart.top = new FormAttachment( lastControl, margin );
    wClearOnStart.setLayoutData( fdClearOnStart );
    lastControl = wClearOnStart;
    
    // 
    // Replace cache content at end of transformation?
    //
    Label wlReplaceAtTransEnd = new Label( wStreamingServiceComp, SWT.LEFT );
    wlReplaceAtTransEnd.setText( BaseMessages.getString( PKG, "TransDialog.ReplaceAtTransEnd.Label" ) );
    wlReplaceAtTransEnd.setToolTipText(BaseMessages.getString( PKG, "TransDialog.ReplaceAtTransEnd.Tooltip" ) );
    props.setLook( wlReplaceAtTransEnd );
    FormData fdlReplaceAtTransEnd = new FormData();
    fdlReplaceAtTransEnd.left = new FormAttachment( 0, 0 );
    fdlReplaceAtTransEnd.right = new FormAttachment( middle, -margin );
    fdlReplaceAtTransEnd.top = new FormAttachment( lastControl, margin );
    wlReplaceAtTransEnd.setLayoutData( fdlReplaceAtTransEnd );
    wReplaceAtTransEnd = new Button( wStreamingServiceComp, SWT.CHECK );
    wReplaceAtTransEnd.setToolTipText(BaseMessages.getString( PKG, "TransDialog.ReplaceAtTransEnd.Tooltip" ) );
    props.setLook( wReplaceAtTransEnd );
    FormData fdReplaceAtTransEnd = new FormData();
    fdReplaceAtTransEnd.left = new FormAttachment( middle, 0 );
    fdReplaceAtTransEnd.right = new FormAttachment( 100, 0 );
    fdReplaceAtTransEnd.top = new FormAttachment( lastControl, margin );
    wReplaceAtTransEnd.setLayoutData( fdReplaceAtTransEnd );
    lastControl = wReplaceAtTransEnd;
 
    // 
    // Log Level
    //
    Label wlLogLevel = new Label( wStreamingServiceComp, SWT.LEFT );
    wlLogLevel.setText( BaseMessages.getString( PKG, "TransDialog.LogLevel.Label" ) );
    wlLogLevel.setToolTipText( BaseMessages.getString( PKG, "TransDialog.LogLevel.Label" ) );
    props.setLook( wlLogLevel );
    FormData fdlLogLevel = new FormData();
    fdlLogLevel.left = new FormAttachment( 0, 0 );
    fdlLogLevel.right = new FormAttachment( middle, -margin );
    fdlLogLevel.top = new FormAttachment( lastControl, margin );
    wlLogLevel.setLayoutData( fdlLogLevel );
    wLogLevel = new CCombo( wStreamingServiceComp, SWT.LEFT | SWT.BORDER | SWT.SINGLE );
    wLogLevel.setToolTipText( BaseMessages.getString( PKG, "TransDialog.LogLevel.Tooltip" ) );
    props.setLook( wLogLevel );
    FormData fdLogLevel = new FormData();
    fdLogLevel.left = new FormAttachment( middle, 0 );
    fdLogLevel.right = new FormAttachment( 100, 0 );
    fdLogLevel.top = new FormAttachment( lastControl, margin );
    wLogLevel.setLayoutData( fdLogLevel );
    wLogLevel.setItems( LogLevel.getLogLevelDescriptions() );
    lastControl = wLogLevel;

    
    Button wNew = new Button( wStreamingServiceComp, SWT.PUSH );
    props.setLook( wNew );
    wNew.setText( BaseMessages.getString( PKG, "TransDialog.NewServiceButton.Label" ) );
    FormData fdNew = new FormData();
    fdNew.left = new FormAttachment( middle, 0 );
    fdNew.top = new FormAttachment( lastControl, margin * 2 );
    wNew.setLayoutData( fdNew );
    wNew.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        String newName = createNewService( shell, transMeta.getMetaStore() );
        if ( newName != null ) {
          wServiceName.setItems( getServiceElementNames( shell, transMeta.getMetaStore() ) );
          wServiceName.setText( newName );
        }
      }
    } );

    Button wRemove = new Button( wStreamingServiceComp, SWT.PUSH );
    props.setLook( wRemove );
    wRemove.setText( BaseMessages.getString( PKG, "TransDialog.RemoveServiceButton.Label" ) );
    FormData fdRemove = new FormData();
    fdRemove.left = new FormAttachment( wNew, margin * 2 );
    fdRemove.top = new FormAttachment( lastControl, margin * 2 );
    wRemove.setLayoutData( fdRemove );
    wRemove.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        boolean removed = removeService( shell, transMeta.getMetaStore(), wServiceName.getText() );
        if ( removed ) {
          wServiceName.setItems( getServiceElementNames( shell, transMeta.getMetaStore() ) );
          wServiceName.setText( "" );
        }

      }
    } );

    FormData fdStreamingServiceComp = new FormData();
    fdStreamingServiceComp.left = new FormAttachment( 0, 0 );
    fdStreamingServiceComp.top = new FormAttachment( 0, 0 );
    fdStreamingServiceComp.right = new FormAttachment( 100, 0 );
    fdStreamingServiceComp.bottom = new FormAttachment( 100, 0 );
    wStreamingServiceComp.setLayoutData( fdStreamingServiceComp );

    wStreamingServiceComp.layout();
    wStreamingServiceTab.setControl( wStreamingServiceComp );
  }

  protected boolean removeService( Shell shell, IMetaStore metaStore, String elementName ) {
    MessageDialog dialog = new MessageDialog( shell, "Confirm removal", shell.getDisplay().getSystemImage( SWT.ICON_QUESTION ),
      "Are you sure you want to remove streaming service '" + elementName + "'?", SWT.NONE, new String[] { "Yes", "No" }, 1 );
    int answerIndex = dialog.open();
    if ( answerIndex == 0 ) {
      try {
        MetaStoreFactory<StreamingService> rtFactory = new MetaStoreFactory<StreamingService>( StreamingService.class, metaStore, PentahoDefaults.NAMESPACE );
        rtFactory.deleteElement( elementName );
        return true;
      } catch ( MetaStoreException e ) {
        new ErrorDialog( shell, "Error", "Error deleting streaming service with name '" + elementName + "'", e );
        return false;
      }

    }

    return false;
  }

  protected String createNewService( Shell shell, IMetaStore metaStore ) {
    EnterStringDialog dialog = new EnterStringDialog( shell, "table1", "Enter service name", "Enter the name of the new streaming service" );
    String name = dialog.open();
    if ( name != null ) {

      try {
        MetaStoreFactory<StreamingService> rtFactory = new MetaStoreFactory<StreamingService>( StreamingService.class, metaStore, PentahoDefaults.NAMESPACE );
        if ( rtFactory.loadElement( name ) != null ) {
          throw new MetaStoreException( "The streaming service with name '" + name + "' already exists" );
        }
      } catch ( MetaStoreException e ) {
        new ErrorDialog( shell, "Error", "Error creating new streaming service", e );
        return null;
      }

      return name;
    } else {
      return null;
    }
  }

  private String[] getServiceElementNames( Shell shell, IMetaStore metaStore ) {
    try {
      MetaStoreFactory<StreamingService> rtFactory = new MetaStoreFactory<StreamingService>( StreamingService.class, metaStore, PentahoDefaults.NAMESPACE );
      List<String> list = rtFactory.getElementNames();
      String[] names = list.toArray( new String[list.size()] );
      Arrays.sort( names );
      return names;
    } catch ( Exception e ) {
      e.printStackTrace();
      new ErrorDialog( shell, "Error", "Error getting list of streaming services", e );
      return new String[] {};
    }
  }

  @Override
  public void getData( TransMeta transMeta ) throws KettleException {
    try {

      String serviceName = transMeta.getAttribute( StreamingConst.STREAMING_GROUP, StreamingConst.STREAMING_SERVICE_NAME );
      if ( Const.isEmpty( serviceName ) ) {
        return;
      }
      MetaStoreFactory<StreamingService> rtFactory = new MetaStoreFactory<StreamingService>( StreamingService.class, transMeta.getMetaStore(), PentahoDefaults.NAMESPACE );
      StreamingService streamingService = rtFactory.loadElement( serviceName );
      if ( streamingService == null ) {
        return;
      }

      wServiceName.setText( Const.NVL( streamingService.getName(), "" ) );
      wServiceStep.setText( Const.NVL( streamingService.getStepname(), "" ) );
      wServiceCacheDuration.setText( Const.NVL( streamingService.getCacheDuration(), "" ) );
      wServiceCacheSize.setText( Const.NVL( streamingService.getCacheSize(), "" ) );
      wPreloadService.setSelection( streamingService.isPreloaded() );
      wClearOnStart.setSelection( streamingService.isClearingOnStart() );
      wReplaceAtTransEnd.setSelection(streamingService.isCacheFlipping());
      LogLevel logLevel = streamingService.getLogLevel()==null ? LogLevel.BASIC : streamingService.getLogLevel();
      wLogLevel.select( logLevel.getLevel() );

    } catch ( Exception e ) {
      throw new KettleException( "Unable to load streaming service", e );
    }
  }

  @Override
  public void ok( TransMeta transMeta ) throws KettleException {

    try {
      // Get streaming service details...
      //
      StreamingService streamingService = new StreamingService();
      streamingService.setName( wServiceName.getText() );
      streamingService.setStepname( wServiceStep.getText() );
      streamingService.setCacheDuration( wServiceCacheDuration.getText() );
      streamingService.setCacheSize( wServiceCacheSize.getText() );
      streamingService.setPreloaded( wPreloadService.getSelection() );
      streamingService.setClearingOnStart( wClearOnStart.getSelection() );
      streamingService.setCacheFlipping(wReplaceAtTransEnd.getSelection());
      streamingService.setLogLevel( LogLevel.values()[wLogLevel.getSelectionIndex()] );

      streamingService.setTransFilename( transMeta.getFilename() );
      Repository repository = transMeta.getRepository();
      if ( repository != null ) {
        if ( repository.getRepositoryMeta().getRepositoryCapabilities().supportsReferences() ) {
          ObjectId objectId = transMeta.getObjectId();
          streamingService.setTransObjectId( objectId == null ? null : objectId.getId() );
        }
        streamingService.setTransRepositoryPath( transMeta.getRepositoryDirectory().getPath() + "/" + transMeta.getName() );
      }

      MetaStoreFactory<StreamingService> rtFactory = new MetaStoreFactory<StreamingService>( StreamingService.class, transMeta.getMetaStore(), PentahoDefaults.NAMESPACE );
      rtFactory.saveElement( streamingService );

      transMeta.setAttribute( StreamingConst.STREAMING_GROUP, StreamingConst.STREAMING_SERVICE_NAME, streamingService.getName() );
      transMeta.setChanged();

    } catch ( Exception e ) {
      throw new KettleException( "Error saveing streaming service metadata", e );
    }

  }

  @Override
  public CTabItem getTab() {
    return wStreamingServiceTab;
  }
}
