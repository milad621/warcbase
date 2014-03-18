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
import java.util.StringTokenizer;

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
import org.warcbase.analysis.NER.*;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import tl.lin.data.SortableEntries.Order;
import tl.lin.data.fd.Object2IntFrequencyDistribution;
import tl.lin.data.fd.Object2IntFrequencyDistributionEntry;
import tl.lin.data.pair.PairOfObjectInt;

public class ExtractText {
  private static final Logger LOG = Logger.getLogger(ExtractText.class);
  private static final String NAME_OPTION = "name";
  private static final String DIR_OPTION = "dir";
  private static final NEFinder neFinder = new NEFinder();

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
=======
    reader.close();

    for (String s : originalIds) {
      if (!ids.contains(s) && !s.equals("go")) {
        ids.add(s);
      }
    }

    /*for(int i=0;i<ids.size();i++){
>>>>>>> forkOrigin/master
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
      int idInd = 0;
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
       
      if (id.equals("frist") || id.equals("dole") || id.equals("specter")){
        String[] splits = keyStr.split("\\/");
        if(splits.length > 3) {
          continue;
        }
          
      }
        
      String filePath = path;// + id;

      Get get = new Get(key);
      Result rs = table.get(get);

      if (rs.raw().length == 0) {
        continue;
      }
      
      type = Bytes.toString(rs.raw()[0].getQualifier());

      if (!(type.contains("html"))) {
        continue;
      }
      for (int i = 0; i < rs.raw().length; i++) {
        content = Bytes.toString(rs.raw()[i].getValue());

        String cleaned = Jsoup.parse(content).text().replaceAll("[\\r\\n]+", " ");
        StringTokenizer st = new StringTokenizer(cleaned);
        List<String> words = Lists.newArrayList();
        while (st.hasMoreElements()) {
          words.add((String) st.nextElement());
        }
        words.removeAll(stop_words);
        cleaned = Joiner.on(" ").join(words);
        cleaned = neFinder.replaceNER(cleaned);
        words = AnalyzerUtils.parse(new SimpleAnalyzer(Version.LUCENE_43), cleaned);
        words.removeAll(stop_words);
        String text = Joiner.on(" ").join(words);
        if(text.length() < 300)
           continue;
        filePath = filePath + "/" + id + "#" + rs.raw()[i].getTimestamp()
            + DigestUtils.sha1Hex(key)+ ".txt";
        if (text.contains("this page cannot be found sorry") || text.contains("this page can not be found sorry")) {
          if (DigestUtils.sha1Hex(key).equals("464dc410d467043792b225612d730fa9")) {
            System.out.println(keyStr);
          }
          if (DigestUtils.sha1Hex(key).equals("876c20659b1a4b8dc3e32868dat6e425")) {
            System.out.println(keyStr);
          }
          if (DigestUtils.sha1Hex(key).equals("a428b5252dt8ea0454e1cb076cb54b0c52faf")) {
            System.out.println(keyStr);
          }
          continue;
        }
        if (DigestUtils.sha1Hex(key).endsWith("e4aebcaf356f1ae6207ed84bf9c9")) {
          System.out.println(keyStr);
        }
        if (DigestUtils.sha1Hex(key).endsWith("e3ba36fd30d8cd62c236ae5aab64bbda651")) {
          System.out.println(keyStr);
        }
        if (DigestUtils.sha1Hex(key).endsWith("c6ad5b56d2a666329dc8de21f2786f80392c")) {
          System.out.println(keyStr);
        }
        if (DigestUtils.sha1Hex(key).endsWith("d2910cfdc180278970d498769c309a968f")) {
          System.out.println(keyStr);
        }
        if (DigestUtils.sha1Hex(key).endsWith("daafaeac51fdd9ff8d695cbef09416941110060")) {
          System.out.println(keyStr);
        }
        FileWriter out = new FileWriter(filePath, true);
        out.write(text);
        out.close();
      }
    }

    pool.close();

  }

}
