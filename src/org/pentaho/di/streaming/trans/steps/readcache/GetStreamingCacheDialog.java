package org.pentaho.di.streaming.trans.steps.readcache;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.streaming.StreamingService;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.ui.core.widget.ComboVar;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.trans.step.BaseStepDialog;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.metastore.persist.MetaStoreFactory;
import org.pentaho.metastore.util.PentahoDefaults;

public class GetStreamingCacheDialog extends BaseStepDialog implements StepDialogInterface {
  private static Class<?> PKG = GetStreamingCacheDialog.class; // i18n
  
  private GetStreamingCacheMeta input;
  
  private ComboVar wServiceName;
  private TextVar wIdField;
  private TextVar wTimeStampField;
  private ComboVar wSlaveServer;
  
  public GetStreamingCacheDialog(Shell parent, Object baseStepMeta, TransMeta transMeta, String stepname) {
    super(parent, (BaseStepMeta)baseStepMeta, transMeta, stepname);
    
    input = (GetStreamingCacheMeta) baseStepMeta;
  }

  @Override
  public String open() {
    
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX );
    props.setLook( shell );
    setShellImage( shell, input );
    
    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "GetStreamingCacheDialog.Shell.Title" ) );

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;
  
    // Step name...
    //
    wlStepname = new Label( shell, SWT.RIGHT );
    wlStepname.setText( BaseMessages.getString( PKG, "GetStreamingCacheDialog.Stepname.Label" ) );
    props.setLook( wlStepname );
    fdlStepname = new FormData();
    fdlStepname.left = new FormAttachment( 0, 0 );
    fdlStepname.right = new FormAttachment( middle, -margin );
    fdlStepname.top = new FormAttachment( 0, margin );
    wlStepname.setLayoutData( fdlStepname );
    wStepname = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wStepname.setText( stepname );
    props.setLook( wStepname );
    fdStepname = new FormData();
    fdStepname.left = new FormAttachment( middle, 0 );
    fdStepname.top = new FormAttachment( 0, margin );
    fdStepname.right = new FormAttachment( 100, 0 );
    wStepname.setLayoutData( fdStepname );
    Control lastControl = wStepname;
    
    // Service name
    //
    Label wlServiceName = new Label( shell, SWT.RIGHT );
    wlServiceName.setText( BaseMessages.getString( PKG, "GetStreamingCacheDialog.ServiceName.Label" ) );
    props.setLook( wlServiceName );
    FormData fdlServiceName = new FormData();
    fdlServiceName.left = new FormAttachment( 0, 0 );
    fdlServiceName.right = new FormAttachment( middle, -margin );
    fdlServiceName.top = new FormAttachment( lastControl, margin );
    wlServiceName.setLayoutData( fdlServiceName );
    wServiceName = new ComboVar( transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wServiceName.setItems(new String[] {}); // set in getData()
    props.setLook( wServiceName );
    FormData fdServiceName = new FormData();
    fdServiceName.left = new FormAttachment( middle, 0 );
    fdServiceName.top = new FormAttachment( lastControl, margin );
    fdServiceName.right = new FormAttachment( 100, 0 );
    wServiceName.setLayoutData( fdServiceName );
    lastControl = wServiceName;
    
    // ID field
    //
    Label wlIdField = new Label( shell, SWT.RIGHT );
    wlIdField.setText( BaseMessages.getString( PKG, "GetStreamingCacheDialog.IdField.Label" ) );
    props.setLook( wlIdField );
    FormData fdlIdField = new FormData();
    fdlIdField.left = new FormAttachment( 0, 0 );
    fdlIdField.right = new FormAttachment( middle, -margin );
    fdlIdField.top = new FormAttachment( lastControl, margin );
    wlIdField.setLayoutData( fdlIdField );
    wIdField = new TextVar( transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wIdField );
    FormData fdIdField = new FormData();
    fdIdField.left = new FormAttachment( middle, 0 );
    fdIdField.top = new FormAttachment( lastControl, margin );
    fdIdField.right = new FormAttachment( 100, 0 );
    wIdField.setLayoutData( fdIdField );
    lastControl = wIdField;
    
    // Timestamp field
    //
    Label wlTimeStampField = new Label( shell, SWT.RIGHT );
    wlTimeStampField.setText( BaseMessages.getString( PKG, "GetStreamingCacheDialog.TimeStampField.Label" ) );
    props.setLook( wlTimeStampField );
    FormData fdlTimeStampField = new FormData();
    fdlTimeStampField.left = new FormAttachment( 0, 0 );
    fdlTimeStampField.right = new FormAttachment( middle, -margin );
    fdlTimeStampField.top = new FormAttachment( lastControl, margin );
    wlTimeStampField.setLayoutData( fdlTimeStampField );
    wTimeStampField = new TextVar( transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wTimeStampField );
    FormData fdTimeStampField = new FormData();
    fdTimeStampField.left = new FormAttachment( middle, 0 );
    fdTimeStampField.top = new FormAttachment( lastControl, margin );
    fdTimeStampField.right = new FormAttachment( 100, 0 );
    wTimeStampField.setLayoutData( fdTimeStampField );
    lastControl = wTimeStampField;
    
    // Slave Server
    //
    Label wlSlaveServer = new Label( shell, SWT.RIGHT );
    wlSlaveServer.setText( BaseMessages.getString( PKG, "GetStreamingCacheDialog.SlaveServer.Label" ) );
    props.setLook( wlSlaveServer );
    FormData fdlSlaveServer = new FormData();
    fdlSlaveServer.left = new FormAttachment( 0, 0 );
    fdlSlaveServer.right = new FormAttachment( middle, -margin );
    fdlSlaveServer.top = new FormAttachment( lastControl, margin );
    wlSlaveServer.setLayoutData( fdlSlaveServer );
    wSlaveServer = new ComboVar( transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wSlaveServer );
    FormData fdSlaveServer = new FormData();
    fdSlaveServer.left = new FormAttachment( middle, 0 );
    fdSlaveServer.top = new FormAttachment( lastControl, margin );
    fdSlaveServer.right = new FormAttachment( 100, 0 );
    wSlaveServer.setLayoutData( fdSlaveServer );
    lastControl = wSlaveServer;

    // Some buttons
    wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
    // Add listeners
    lsCancel = new Listener() {
      public void handleEvent( Event e ) {
        cancel();
      }
    };
    lsOK = new Listener() {
      public void handleEvent( Event e ) {
        ok();
      }
    };

    wCancel.addListener( SWT.Selection, lsCancel );
    wOK.addListener( SWT.Selection, lsOK );
    setButtonPositions( new Button[] { wOK, wCancel }, margin, null );

    
    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wServiceName.addSelectionListener( lsDef );
    wIdField.addSelectionListener( lsDef );
    wTimeStampField.addSelectionListener( lsDef );
    wSlaveServer.addSelectionListener( lsDef );
    
    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    // Set the shell size, based upon previous time...
    setSize();

    getData();
    input.setChanged( changed );

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return stepname;
  }

  private void getData() {
    
    try {
      MetaStoreFactory<StreamingService> factory = new MetaStoreFactory<StreamingService>(StreamingService.class, metaStore, PentahoDefaults.NAMESPACE);
      List<String> names = factory.getElementNames();
      Collections.sort(names);
      wServiceName.setItems(names.toArray(new String[names.size()]));
    } catch(MetaStoreException e) {
      LogChannel.GENERAL.logError("Error reading streaming service names from the metastore", e);
    }
    
    wServiceName.setText( Const.NVL(input.getServiceName(), ""));
    wIdField.setText(Const.NVL(input.getIdField(), ""));
    wTimeStampField.setText(Const.NVL(input.getTimestampField(), ""));
    
    String[] slaves = transMeta.getSlaveServerNames();
    Arrays.sort(slaves);
    wSlaveServer.setItems(slaves);
    wSlaveServer.setText(Const.NVL(input.getSlaveServer(), ""));
    
    wStepname.selectAll();
    wStepname.setFocus();
  }

  private void cancel() {
    stepname = null;
    input.setChanged( changed );
    dispose();
  }

  private void ok() {
    if ( Const.isEmpty( wStepname.getText() ) ) {
      return;
    }

    stepname = wStepname.getText(); // return value

    input.setChanged();
    
    input.setServiceName( wServiceName.getText() );
    input.setIdField( wIdField.getText() );
    input.setTimestampField( wTimeStampField.getText() );
    input.setSlaveServer( wSlaveServer.getText() );
    
    dispose();
  }
}
