package nearsoft.academy.bigdata.recommendation;

import org.apache.hadoop.util.StringUtils;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.model.GenericDataModel;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.ThresholdUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.UserBasedRecommender;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Scanner;
import java.util.zip.GZIPInputStream;

public class MovieRecommender {


    //DataModel model = new FileDataModel(new File("/home/ocrisostomo/Downloads/movies.txt"));
   // UserSimilarity similarity = new PearsonCorrelationSimilarity(model);
    //UserNeighborhood neighborhood = new ThresholdUserNeighborhood(0.1,similarity,model);
    //UserBasedRecommender recommender = new GenericUserBasedRecommender(model,neighborhood,similarity);

    public MovieRecommender() throws IOException, TasteException {

    }

   /* public int getTotalReviews(){
        int totalItems=0;
        try {
   //         totalItems= model.getNumItems();
        } catch (TasteException e) {
            e.printStackTrace();
        }
        System.out.println(totalItems);
        return totalItems;
    }*/


    public static List read() throws IOException {
        HashMap<String,Integer> mapBackPel = new HashMap<String,Integer >();
        HashMap<Integer,String>  mapGoPel = new HashMap<Integer,String>();
        HashMap<String,Integer> mapBackUs = new HashMap<String,Integer >();
        HashMap<Integer,String>  mapGoUs = new HashMap<Integer,String>();

        GZIPInputStream inputStream = null;
        int cont = 1;
        int cont2=1;
        List<String> plainData = new ArrayList();
        Scanner sc = null;
        try {
            inputStream = new GZIPInputStream(new FileInputStream("/home/ocrisostomo/Downloads/movies.txt.gz"));
            sc = new Scanner(inputStream, "UTF-8");
            String toAdd= new String();


            while (sc.hasNextLine()) {
                String line = sc.nextLine();
               // System.out.println(line);
                if(org.apache.commons.lang.StringUtils.isNotBlank(line)){
                    String [] aux = line.split(" ");

                    if(aux[0].equals("product/productId:")){
                        if(mapGoPel.isEmpty()){
                            mapGoPel.put(cont,aux[1]);
                            mapBackPel.put(aux[1],cont);
                            toAdd = cont+",";
                        } else if(mapBackPel.containsKey(aux[1])){
                            toAdd+=cont + ",";
                        }else {
                            cont++;
                            mapGoPel.put(cont, aux[1]);
                            mapBackPel.put(aux[1], cont);
                            toAdd += cont + ",";
                        }
                    } else if(aux[0].equals("review/userId:")){

                        if(mapGoUs.isEmpty() || mapBackUs.isEmpty()){
                            mapGoUs.put(cont2,aux[1]);
                            mapBackUs.put(aux[1],cont2);
                            toAdd += cont2+",";
                            continue;
                        } else if(mapBackUs.containsKey(aux[1])){
                            //toAdd+= cont2 + ",";
                            toAdd+=mapBackUs.get(aux[1])+",";

                        }else{

                            mapGoUs.put(cont2,aux[1]);
                            mapBackUs.put(aux[1],cont2);
                            toAdd+=cont2 + ",";
                            cont2++;
                        }

                    } else if(aux[0].equals("review/score:")){
                        toAdd += aux[1];
                    }




                }else {
                    plainData.add(toAdd);
                    toAdd="";
                    System.out.println(plainData.size());
                }

            }

            if (sc.ioException() != null) {
                throw sc.ioException();
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (inputStream != null) {
                inputStream.close();
            }
            if (sc != null) {
                sc.close();
            }
        }
        return plainData;
    }



    public void getFIle(List<String> list) {

        FileWriter fl = null;
        PrintWriter pw = null;

        try {
            fl = new FileWriter("/home/ocrisostomo/Downloads/movies.txt");
            pw = new PrintWriter(fl);
            for (String obj : list) {
                pw.println(obj);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (null != fl) {
                try {
                    fl.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }

            }
        }


    }




}
