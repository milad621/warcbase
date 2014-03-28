package org.warcbase.data;

import java.io.IOException;
import java.lang.reflect.Field;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.hfile.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.warcbase.ingest.IngestFiles;

public class HbaseManager {
  private static final String[] FAMILIES = { "c"};
  private static final Logger LOG = Logger.getLogger(HbaseManager.class);
  private static final int MAX_KEY_VALUE_SIZE = IngestFiles.MAX_CONTENT_SIZE + 200;
  public static final int MAX_VERSIONS = Integer.MAX_VALUE;

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
        hColumnDesc.setCompressionType(Algorithm.SNAPPY);
        hColumnDesc.setCompactionCompressionType(Algorithm.SNAPPY);
        hColumnDesc.setTimeToLive(HConstants.FOREVER);
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
      Put put = new Put(Bytes.toBytes(key));
      put.setWriteToWAL(false);
      put.add(Bytes.toBytes(FAMILIES[0]), Bytes.toBytes(type), timestamp.getTime(), data);
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

  public static void main(String[] args) throws ParseException, IOException, SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException {
    /*SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddhhmmss");
    java.util.Date parsedDate = dateFormat.parse("20040124034300");
    Timestamp timestamp = new java.sql.Timestamp(parsedDate.getTime());
    System.out.println(timestamp.getTime());*/
    /*String name = "test-3-24";
    Configuration hbaseConfig = HBaseConfiguration.create();
    HBaseAdmin admin = new HBaseAdmin(hbaseConfig);
    if (admin.tableExists(name)) {
      admin.disableTable(name);
      admin.deleteTable(name);
    }
    HTableDescriptor tableDesc = new HTableDescriptor(name);
    String family = "c";
    HColumnDescriptor hColumnDesc = new HColumnDescriptor(family);
    hColumnDesc.setMaxVersions(4);
    hColumnDesc.setTimeToLive(Integer.MAX_VALUE);
    System.out.println(hColumnDesc.DEFAULT_TTL);
    System.out.println(hColumnDesc.getTimeToLive());
    hColumnDesc.setTimeToLive(2147483647);
    tableDesc.addFamily(hColumnDesc);
    admin.createTable(tableDesc);
    HTable table = new HTable(hbaseConfig, name);
    Field maxKeyValueSizeField = HTable.class.getDeclaredField("maxKeyValueSize");
    maxKeyValueSizeField.setAccessible(true);
    maxKeyValueSizeField.set(table, MAX_KEY_VALUE_SIZE);
    admin.close();
    Put put = new Put(Bytes.toBytes("key"));
    put.setWriteToWAL(false);
    put.add(Bytes.toBytes(family), Bytes.toBytes("q0"), 0, Bytes.toBytes("v0"));
    put.add(Bytes.toBytes(family), Bytes.toBytes("q0"), 1, Bytes.toBytes("v1"));
    put.add(Bytes.toBytes(family), Bytes.toBytes("q0"), 2, Bytes.toBytes("v2"));
    put.add(Bytes.toBytes(family), Bytes.toBytes("q0"), 3, Bytes.toBytes("v3"));
    table.put(put);
    
    
    HTableInterface hTableInterface = pool.getTable(name);
    Get get = new Get(Bytes.toBytes("key"));
    get.setMaxVersions(4);
    Result rs = hTableInterface.get(get);
    System.out.println(rs.raw().length);*/
    /*Configuration conf = HBaseConfiguration.create();

    HBaseHelper helper = HBaseHelper.getHelper(conf);
    helper.dropTable("testtable");
    helper.createTable("testtable", "colfam1");
    HTable table = new HTable(conf, "testtable");

    // vv PutIdenticalExample
    Put put = new Put(Bytes.toBytes("row1"));
    put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"),
      Bytes.toBytes("val2"));
    put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"),
      Bytes.toBytes("val1")); // co PutIdenticalExample-1-Add Add the same column with a different value. The last value is going to be used.
    table.put(put);

    Get get = new Get(Bytes.toBytes("row1"));
    get.setMaxVersions(3);
    Result result = table.get(get);
    System.out.println("Result: " + result + ", Value: " + Bytes.toString(
      result.getValue(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1")))); // co PutIdenticalExample-2-Get Perform a get to verify that "val1" was actually stored.
    System.out.println(result.raw().length);*/
  }
}

