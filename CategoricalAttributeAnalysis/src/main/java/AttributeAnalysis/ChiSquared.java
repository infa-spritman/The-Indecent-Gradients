package AttributeAnalysis;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

public class ChiSquared {
    public static void main(String[] args) throws IOException {

    }

    public static HashMap<Integer, HashMap<String, HashMap<Integer, Double>>> readFile(String fileAddress) throws IOException {
        HashMap<Integer, HashMap<String, HashMap<Integer, Double>>> ans = new HashMap<>();

        BufferedReader br = new BufferedReader(new FileReader(fileAddress));
        String line = br.readLine();
        while(line != null){
            String[] splitLine = line.split(",");
            int index = Integer.parseInt(splitLine[0]);
            HashMap<String, HashMap<Integer, Double>> columnMap = new HashMap<>();
            if(ans.containsKey(index)){
                columnMap = ans.get(index);
            }
            else{
                ans.put(index, columnMap);
            }
            HashMap<Integer, Double> domainMap = new HashMap<>();
            if(columnMap.containsKey(splitLine[1])){
                domainMap = columnMap.get(splitLine[1]);
            }
            else{
                columnMap.put(splitLine[1], domainMap);
            }

            domainMap.put(Integer.parseInt(splitLine[2]), Double.parseDouble(splitLine[3]));

            line = br.readLine();
        }
        return ans;
    }
}
