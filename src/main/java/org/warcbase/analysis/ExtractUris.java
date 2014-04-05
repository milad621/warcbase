package org.warcbase.analysis;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.warcbase.data.UriMapping;
import org.warcbase.data.Util;

public class ExtractUris {
  private static final Logger LOG = Logger.getLogger(ExtractUris.class);
  private static final String NAME_OPTION = "name";
  
  private static void createCsvFromFile(File f) throws IOException{
    UriMapping mapping = new UriMapping("senateUrls.fst");
    PrintWriter writer = new PrintWriter("senate_uri_mapping.csv", "UTF-8");
    String contents = FileUtils.readFileToString(f);
    String[] lines = contents.split("\\n");
    for (String line : lines) {
      // This need to modify according to your input file
      if (!line.equals("")) { // non-empty string
        String url = line.split("\\s+")[0];
        int mapId = mapping.getID(url);
        //writer.println("\"" + mapId + "\"" + ", " + "\"" + url + "\"");
        writer.println(mapId+ ", " + url);
      }
    }
    writer.close();
  }
  
  public static void main(String[] args) throws IOException {
    createCsvFromFile(new File("senate_uris.txt"));
    if(true)return;
    Options options = new Options();
    options.addOption(OptionBuilder.withArgName("name").hasArg()
        .withDescription("name of the archive").create(NAME_OPTION));
    
    CommandLine cmdline = null;
    CommandLineParser parser = new GnuParser();
    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      System.exit(-1);
    }
    
    if (!cmdline.hasOption(NAME_OPTION)) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(ExtractText.class.getCanonicalName(), options);
      System.exit(-1);
    }
    String name = cmdline.getOptionValue(NAME_OPTION);
    
    Map<String, String> idUri = new HashMap<String, String>();
    String csvFile = "senate_id_url.csv";//"id_url_2.csv";
    String line = "";
    BufferedReader br = new BufferedReader(new FileReader(csvFile));
    while ((line = br.readLine()) != null) {
      String[] splits = line.split(",");
      for (int i = 1; i < splits.length; i++) {
        String uri = splits[i];
        while (uri.startsWith(" ")) {
          uri = uri.substring(1);
        }
        idUri.put(uri, splits[0]);
      }
    }
    br.close();

    List<String> ids = new ArrayList<String>(100);
    for (Map.Entry<String, String> entry : idUri.entrySet()) {
      if (!ids.contains(entry.getValue())) {
        ids.add(entry.getValue());
      }
    }
    
    HTablePool pool = new HTablePool();
    HTableInterface table = pool.getTable(name);
    Scan scan = new Scan();
    scan.setFilter(new FirstKeyOnlyFilter());
    ResultScanner scanner = null;
    scanner = table.getScanner(scan);
    UriMapping mapping = new UriMapping("senateUrls.fst");
    PrintWriter writer = new PrintWriter("senate_uri_mapping.csv", "UTF-8");
    PrintWriter writer1 = new PrintWriter("senate_uris.txt", "UTF-8");
    
    for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
      byte[] key = rr.getRow();
      String id = "";
      String keyStr = Bytes.toString(key);
      boolean ambiguous = false;
      for (Map.Entry<String, String> entry : idUri.entrySet()) {
        if (keyStr.startsWith(entry.getKey())) {
          if (!id.equals("") && !id.equals(entry.getValue())) {
            LOG.warn(id + " " + entry.getValue());
            ambiguous = true;
          }
          id = entry.getValue();
        }
      }
      
      if (id.equals("") || ambiguous) {
        continue;
      }
      
      String uri = Util.reverseBacUri(keyStr);
      int mapId = mapping.getID(uri);
      //System.out.println(keyStr + " " + uri + " " + mapId);
      writer.println(mapId + ", " + uri);
      writer1.println(uri);
    }
    writer1.close();
    writer.close();
    pool.close();
  }
}
