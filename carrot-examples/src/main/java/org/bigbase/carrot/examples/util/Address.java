package org.bigbase.carrot.examples.util;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class Address extends KeyValues{

  Address(Properties p) {
    super(p);
  }

  static String[] ATTRIBUTES = new String[] {
      "LON","LAT","NUMBER","STREET", "UNIT", "CITY", "DISTRICT","REGION","POSTCODE","ID","HASH"
  };
  
  @SuppressWarnings("deprecation")
  
  public static List<Address> loadFromFile(String name) throws IOException{
    // We filter ill-formatted and those which are missing
    
    FileInputStream fis = new FileInputStream(name);
    DataInputStream dis = new DataInputStream(fis);
    List<Address> list = new ArrayList<Address>();
    
    String line = null;
    long total = 0;
    long valid = 0;
    long totalSize = 0;
    while( (line = dis.readLine()) != null) {
      total++;
      String[] arr = line.split(",");
      if (arr.length != ATTRIBUTES.length) {
        continue;
      }
      if (arr[3].length() == 0 || arr[5].length() == 0 ||
          arr[7].length() == 0 || arr[8].length() == 0) {
        continue;
      }
      valid++;
      Properties p = new Properties();
      // Skip LON, LAT and last two
      for (int i = 2; i < arr.length -2 ; i++) {
        if (arr[i].length() == 0) continue;
        p.put(ATTRIBUTES[i], arr[i]);
        totalSize += ATTRIBUTES[i].length() + arr[i].length(); 
      }
      list.add(new Address(p));
      if ((list.size() % 10000) == 0) {
        System.out.println("Loaded " + list.size());
      }
    }
    dis.close();
    System.out.println("Parsed file: " + name +"\nTotal records="+ total + "\nValid records="+ valid +
      "\nTotalSize="+ totalSize);
    return list;
  }
  
  public static String getUserId(int n) {
    return "address:user:" + n;
  }

  @Override
  public String getKey() {
    // TODO Auto-generated method stub
    return null;
  }
}
