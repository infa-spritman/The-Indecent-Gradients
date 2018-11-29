package AttributeAnalysis;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

public class ChiSquared {
    public static void main(String[] args) throws IOException{
        HashMap<Integer, HashMap<String, HashMap<Integer, Long>>> expected = readFile(args[0]);
        HashMap<Integer, HashMap<String, HashMap<Integer, Long>>> observed = readFile(args[1]);

        Double[] chiValues = new Double[expected.size() + 1];

        for(Map.Entry<Integer, HashMap<String, HashMap<Integer, Long>>> entry0 : expected.entrySet()){
            double chiValue = 0.0;
            HashMap<String, HashMap<Integer, Long>> observedEntry0 = observed.get(entry0.getKey());
            for(Map.Entry<String, HashMap<Integer, Long>> entry1 : entry0.getValue().entrySet()){
                HashMap<Integer, Long> observedEntry1 = observedEntry0.get(entry1.getKey());

                for(Map.Entry<Integer, Long> entry2 : entry1.getValue().entrySet()){
                    Long observedEntry2 = observedEntry1.get(entry2.getKey());
                    chiValue += Math.pow((observedEntry2 - entry2.getValue()),2)/entry2.getValue();
                }
            }
            chiValues[entry0.getKey()] = chiValue;
        }

        PrintWriter write = new PrintWriter("chi.txt", "UTF-8");

        for(int i = 1; i < chiValues.length; i++){
            write.println(i + "," + chiValues[i-1]);
        }
        write.close();
    }

    public static HashMap<Integer, HashMap<String, HashMap<Integer, Long>>> readFile(String fileAddress) throws IOException {
        HashMap<Integer, HashMap<String, HashMap<Integer, Long>>> ans = new HashMap<>();

        BufferedReader br = new BufferedReader(new FileReader(fileAddress));
        String line = br.readLine();
        while(line != null){
            String[] splitLine = line.split(",");
            int index = Integer.parseInt(splitLine[0]);
            HashMap<String, HashMap<Integer, Long>> columnMap = new HashMap<>();
            if(ans.containsKey(index)){
                columnMap = ans.get(index);
            }
            else{
                ans.put(index, columnMap);
            }
            HashMap<Integer, Long> domainMap = new HashMap<>();
            if(columnMap.containsKey(splitLine[1])){
                domainMap = columnMap.get(splitLine[1]);
            }
            else{
                columnMap.put(splitLine[1], domainMap);
            }

            domainMap.put(Integer.parseInt(splitLine[2]), Long.parseLong(splitLine[3]));

            line = br.readLine();
        }
        return ans;
    }
}
