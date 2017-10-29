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
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.UserBasedRecommender;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Scanner;
import java.util.zip.GZIPInputStream;

public class MovieRecommender {

    int  totalRe=0;
   DataModel model;
    UserSimilarity similarity;
    UserNeighborhood neighborhood;
    UserBasedRecommender recommender;

    HashMap<String,Integer> mapKeyMov = new HashMap<String,Integer >();
    HashMap<Integer,String>  mapValMov = new HashMap<Integer,String>();
    HashMap<String,Integer> mapKeyUs = new HashMap<String,Integer >();
    HashMap<Integer,String>  mapValUs = new HashMap<Integer,String>();

    public MovieRecommender(String s) throws IOException, TasteException {
        read();
        model = new FileDataModel(new File(s));
        similarity = new PearsonCorrelationSimilarity(model);
        neighborhood = new ThresholdUserNeighborhood(0.1,similarity,model);
        recommender = new GenericUserBasedRecommender(model,neighborhood,similarity);
    }

    public int getTotalReviews() throws TasteException {
        return totalRe;
    }

    public int getTotalProducts() throws TasteException {

        return model.getNumItems();
    }

    public int getTotalUsers() throws TasteException {
        return model.getNumUsers();
    }

    public List <String> getRecommendationsForUser(String user) throws TasteException {
          List<String> out = new ArrayList<String>();
          int userK =0;
          List<String> recomendation = new ArrayList<String>();
          //List recomenInt;
          //userK = mapBackUs.get(user);
         // recomenInt = recommender.recommend(userK,3);

          List<RecommendedItem> ls = recommender.recommend(userK,3);
        System.out.println(ls.size());

        //  for (Object obj: recomenInt ){
         //     recomendation.add(mapGoPel.get(obj));
         // }
          for(RecommendedItem recIt: ls){
              System.out.println(recIt);
              System.out.println((int) recIt.getItemID());
              System.out.println((int) recIt.getValue());
              System.out.println(String.valueOf(recIt.getItemID()));
              System.out.println(String.valueOf(recIt.getValue()));
              //System.out.println(mapGoPel.get((int) recIt.getItemID()));
             //recomendation.add(mapGoPel.get((int) recIt.getItemID()));
          }

        return recomendation;
    }



   public  int  read() throws IOException {

        FileWriter fl = new FileWriter("/home/ocrisostomo/Downloads/movies.txt");
        PrintWriter pw =new  PrintWriter(fl);
        GZIPInputStream inputStream = null;
        String score;
        int userIdI=0,movieIdI=0;
        int keyMov = 0;
        int keyUs=0;
        Scanner sc = null;

        try {
            inputStream = new GZIPInputStream(new FileInputStream("/home/ocrisostomo/Downloads/movies.txt.gz"));// read file
            sc = new Scanner(inputStream, "UTF-8");
            String toAdd= new String();

            while (sc.hasNextLine()) {
                String line = sc.nextLine();

                if(org.apache.commons.lang.StringUtils.isNotBlank(line)){
                    String [] aux = line.split(" ");


                    if(aux[0].equals("product/productId:")) {

                        if (!mapKeyMov.containsKey(aux[1]) || mapKeyMov.isEmpty()) {
                            keyMov++;
                            mapKeyMov.put(aux[1], keyMov);
                            mapValMov.put(keyMov, aux[1]);
                            movieIdI =  mapKeyMov.get(aux[1]);

                        } else {
                            mapKeyMov.put(aux[1], keyMov);
                            mapValMov.put(keyMov, aux[1]);
                            movieIdI =  mapKeyMov.get(aux[1]);
                        }

                    }else if(aux[0].equals("review/userId:")){

                        if(!mapKeyUs.containsKey(aux[1]) || mapKeyUs.isEmpty()){
                            keyUs++;
                            mapKeyUs.put(aux[1], keyUs);
                            mapValUs.put(keyUs, aux[1]);
                            userIdI =  mapKeyUs.get(aux[1]);

                        } else {

                            userIdI =  mapKeyUs.get(aux[1]);

                        }

                    } else if(aux[0].equals("review/score:")){
                        score = aux[1];
                        toAdd = userIdI +","+movieIdI+","+score;
                        String [] datos = null;
                        datos= toAdd.split(",");

                        if(datos.length== 3 ){
                           // pw.println(toAdd);
                            fl.write(toAdd+"\n");
                            toAdd="";
                            totalRe++;
                        }
                    }
                }

            }


        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        pw.close();

      return totalRe;
    }

}
