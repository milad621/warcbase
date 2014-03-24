package org.warcbase.data;

import java.lang.reflect.Field;
import java.sql.Date;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.warcbase.ingest.IngestFiles;

public class HbaseManager {
  private static final String[] FAMILIES = { "c"};
  private static final Logger LOG = Logger.getLogger(HbaseManager.class);
  private static final int MAX_KEY_VALUE_SIZE = IngestFiles.MAX_CONTENT_SIZE + 200;
  public static final int MAX_VERSIONS = 20;

  private final HTable table;
  private final HBaseAdmin admin;
  private static HTablePool pool = new HTablePool();

  public HbaseManager(String name, boolean create) throws Exception {
    Configuration hbaseConfig = HBaseConfiguration.create();
    admin = new HBaseAdmin(hbaseConfig);

    if (admin.tableExists(name) && !create) {
      LOG.info(String.format("Table '%s' exists: doing nothing.", name));
    } else {
      if (admin.tableExists(name)) {
        LOG.info(String.format("Table '%s' exists: dropping table and recreating.", name));
        LOG.info(String.format("Disabling table '%s'", name));
        admin.disableTable(name);
        LOG.info(String.format("Droppping table '%s'", name));
        admin.deleteTable(name);
      }

      HTableDescriptor tableDesc = new HTableDescriptor(name);
      for (int i = 0; i < FAMILIES.length; i++) {
        //tableDesc.addFamily(new HColumnDescriptor(FAMILIES[i]));
        HColumnDescriptor hColumnDesc = new HColumnDescriptor(FAMILIES[i]);
        hColumnDesc.setMaxVersions(MAX_VERSIONS);
        tableDesc.addFamily(hColumnDesc);
      }
      admin.createTable(tableDesc);
      LOG.info(String.format("Successfully created table '%s'", name));
    }

    table = new HTable(hbaseConfig, name);
    // TODO: This doesn't seem right.
    // Look in HBase book to see how you can set table parameters programmatically.
    Field maxKeyValueSizeField = HTable.class.getDeclaredField("maxKeyValueSize");
    maxKeyValueSizeField.setAccessible(true);
    maxKeyValueSizeField.set(table, MAX_KEY_VALUE_SIZE);

    LOG.info("Setting maxKeyValueSize to " + maxKeyValueSizeField.get(table));
    admin.close();
  }

  public boolean addRecord(String key, String date, byte[] data, String type) {
    try {
      SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddhhmmss");
      java.util.Date parsedDate = dateFormat.parse(date);
      Timestamp timestamp = new java.sql.Timestamp(parsedDate.getTime());
      HTableInterface hTableInterface = pool.getTable("test-3-23");
      Get get = new Get(Bytes.toBytes(key));
      Result rs = hTableInterface.get(get);
      if (key.equals("gov.house.www/")){
        System.out.println("=========================================");
        //System.out.println(date + " " + timestamp + " " + timestamp.getTime());
        
        System.out.println(rs.raw().length);
        if (rs.raw().length == 1)
          System.out.println(rs.raw()[0].getTimestamp());
        //if (rs.raw().length > 0 && timestamp.getTime() < rs.raw()[0].getTimestamp()) {
          //timestamp = new Timestamp(rs.raw()[0].getTimestamp() + 1);
        //}
        //System.out.println(table.getConfiguration().);
        System.out.println();
      }
      Put put = new Put(Bytes.toBytes(key));
      //put.setWriteToWAL(false);
      put.add(Bytes.toBytes(FAMILIES[0]), Bytes.toBytes(type + " " + timestamp.getTime()), timestamp.getTime(), data);
      //put.add(Bytes.toBytes(FAMILIES[1]), Bytes.toBytes(date), Bytes.toBytes(type));
      table.put(put);
      return true;
    } catch (Exception e) {
      LOG.error("Couldn't insert key: " + key);
      LOG.error("File Size: " + data.length);
      LOG.error(e.getMessage());
      e.printStackTrace();
      return false;
    }
  }

  public static void main(String[] args) throws ParseException {
    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddhhmmss");
    java.util.Date parsedDate = dateFormat.parse("20040124034300");
    Timestamp timestamp = new java.sql.Timestamp(parsedDate.getTime());
    System.out.println(timestamp.getTime());
  }
}

