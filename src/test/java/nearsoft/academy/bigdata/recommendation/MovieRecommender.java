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

    HashMap<String,Integer> mapBackPel = new HashMap<String,Integer >();
    HashMap<Integer,String>  mapGoPel = new HashMap<Integer,String>();
    HashMap<String,Integer> mapBackUs = new HashMap<String,Integer >();
    HashMap<Integer,String>  mapGoUs = new HashMap<Integer,String>();

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
        return model.getNumUsers();
    }

    public int getTotalUsers() throws TasteException {
        return model.getNumItems();
    }

    public List <String> getRecommendationsForUser(String user) throws TasteException {
          List<String> out = new ArrayList<String>();
          int userK =0;
          List<String> recomendation = null;
          //List recomenInt;
          userK = mapBackUs.get(user);
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

             recomendation.add(mapGoPel.get((int) recIt.getItemID()));
          }

        return recomendation;
    }



   public  void  read() throws IOException {

      FileWriter fl = new FileWriter("/home/ocrisostomo/Downloads/movies.txt");
       PrintWriter pw =new  PrintWriter(fl);
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
                if(org.apache.commons.lang.StringUtils.isNotBlank(line)){
                    String [] aux = line.split(" ");
                    if(aux[0].equals("product/productId:")){
                        if(mapGoPel.isEmpty()){
                            mapGoPel.put(cont,aux[1]);
                            mapBackPel.put(aux[1],cont);
                            toAdd = cont+",";
                            continue;
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
                        } else if(mapBackUs.containsKey(aux[1])){
                            toAdd+=mapBackUs.get(aux[1])+",";
                        }else{
                            mapGoUs.put(cont2,aux[1]);
                            mapBackUs.put(aux[1],cont2);
                            toAdd+=cont2 + ",";
                            cont2++;
                        }

                    } else if(aux[0].equals("review/score:")){
                        toAdd += aux[1];
                        String [] datos = null;
                        datos= toAdd.split(",");
                      if(datos.length== 3 ){
                          pw.println(toAdd);
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


    }

}
