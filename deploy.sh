pxf cluster stop
rm $PXF_BASE/logs/pxf-service.log
rm $PXF_BASE/logs/*.out
cp target/pxf-delta-connector-1.0-SNAPSHOT.jar $PXF_BASE/lib
pxf cluster start
