package org.warcbase.analysis;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
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
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.util.Version;
import org.jsoup.Jsoup;

import com.google.common.base.Joiner;

//import com.esotericsoftware.minlog.Log;

public class ExtractText {
  private static final Logger LOG = Logger.getLogger(ExtractText.class);
  private static final String NAME_OPTION = "name";
  private static final String DIR_OPTION = "dir";

  @SuppressWarnings("static-access")
  public static void main(String[] args) throws IOException, NoSuchAlgorithmException {
    /*
     * Map<String, String> map = new HashMap<String,String>(); map.put("A", "B"); for
     * (Map.Entry<String, String> entry : map.entrySet()){ System.out.println(entry.getKey() + "/" +
     * entry.getValue()); }
     */

    // if (true) return;
    Options options = new Options();
    options.addOption(OptionBuilder.withArgName("name").hasArg()
        .withDescription("name of the archive").create(NAME_OPTION));
    options.addOption(OptionBuilder.withArgName("dir").hasArg()
        .withDescription("WARC files location").create(DIR_OPTION));

    CommandLine cmdline = null;
    CommandLineParser parser = new GnuParser();
    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      System.exit(-1);
    }

    if (!cmdline.hasOption(DIR_OPTION) || !cmdline.hasOption(NAME_OPTION)) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(ExtractText.class.getCanonicalName(), options);
      System.exit(-1);
    }
    String name = cmdline.getOptionValue(NAME_OPTION);
    String path = cmdline.getOptionValue(DIR_OPTION);

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

    //creating dirctroies for each ID
    /*for (int i = 0; i < ids.size(); i++) {
      File folder = new File(path + ids.get(i));
      if (!folder.exists()) {
        folder.mkdirs();
      }
    }*/

    // Creating stop_words list
    ArrayList<String> stop_words = new ArrayList<String>();
    BufferedReader reader = new BufferedReader(new FileReader("stop_words.txt"));
    line = null;
    while ((line = reader.readLine()) != null) {
      stop_words.add(line);
    }
    reader.close();

    HTablePool pool = new HTablePool();
    HTableInterface table = pool.getTable(name);
    Scan scan = new Scan();
    scan.setFilter(new FirstKeyOnlyFilter());
    ResultScanner scanner = null;
    scanner = table.getScanner(scan);

    String type = "";
    String content = "";
    //System.out.println(idUri.size());
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
      //System.out.println(id + " " + ambiguous);
      if (id.equals("") || ambiguous) {
        continue;
      }

      String filePath = path;// + id;

      Get get = new Get(key);
      Result rs = table.get(get);

      for (int i = 0; i < rs.raw().length; i++) {
        if ((new String(rs.raw()[i].getFamily(), "UTF8").equals("type"))) {
          type = Bytes.toString(rs.raw()[i].getValue());
        }

      }
      if (!(type.contains("html"))) {
        continue;
      }
      for (int i = 0; i < rs.raw().length; i++) {
        if (!(new String(rs.raw()[i].getFamily(), "UTF8").equals("content"))) {
          continue;
        }
        content = Bytes.toString(rs.raw()[i].getValue());
        String cleaned = Jsoup.parse(content).text().replaceAll("[\\r\\n]+", " ");
        List<String> words = AnalyzerUtils.parse(new SimpleAnalyzer(Version.LUCENE_43), cleaned);
        words.removeAll(stop_words);
        String text = Joiner.on(" ").join(words);
        if(text.length() < 300)
           continue;
        filePath = filePath + "/" + id + "#" + Bytes.toString(rs.raw()[i].getQualifier())
            + DigestUtils.sha1Hex(key)+ ".txt";
        FileWriter out = new FileWriter(filePath, true);
        out.write(text);
        out.close();
      }
    }

    pool.close();
  }

}
