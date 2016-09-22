package nearsoft.academy.bigdata.recommendation;

import org.apache.mahout.cf.taste.common.TasteException;
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
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class MovieRecommender {

    private HashMap<String, Integer> productsMap;
    private HashMap<String, Integer> usersMap;
    private File parsedFile;
    private File productsFile;
    private File usersFile;
    private DataModel dataModel;
    private String path;

    public MovieRecommender(String path) throws IOException, ClassNotFoundException {
        this.path = path;
        parsedFile = new File("parsed_" + path + ".csv");
        productsFile = new File("productsMap.dat");
        usersFile = new File("usersMap.dat");
        if (parsedFile.exists() && productsFile.exists() && usersFile.exists()) {
            loadDataFromFile();
        } else {
            parseOriginalFile(path);
        }
    }

    public long getTotalReviews() throws IOException {
        return Files.lines(new File("parsed_" + path + ".csv").toPath()).count();
    }

    public long getTotalProducts() {
        return productsMap.size();
    }

    public long getTotalUsers() {
        return usersMap.size();
    }

    public List<String> getRecommendationsForUser(String user) throws TasteException, IOException {
        int id = usersMap.get(user);
        this.dataModel = new FileDataModel(parsedFile);
        UserSimilarity similarity = new PearsonCorrelationSimilarity(this.dataModel);
        UserNeighborhood neighborhood = new ThresholdUserNeighborhood(0.1, similarity, this.dataModel);
        UserBasedRecommender recommender = new GenericUserBasedRecommender(this.dataModel, neighborhood, similarity);
        List<RecommendedItem> recommendation = recommender.recommend(id, 3);
        List<String> output = new ArrayList<>();
        for (RecommendedItem item : recommendation) {
            for (HashMap.Entry<String, Integer> e : productsMap.entrySet()) {
                if (e.getValue() == item.getItemID()) {
                    output.add(e.getKey());
                    break;
                }
            }
        }
        return output;
    }

    private void parseOriginalFile(String path) throws IOException {
        productsMap = new HashMap<String, Integer>();
        usersMap = new HashMap<String, Integer>();
        BufferedReader br = new BufferedReader(new FileReader(new File(path)));
        FileWriter fw = new FileWriter(parsedFile.getAbsoluteFile());
        int userIndex = 0, productIndex = 0, productValue = -1, userValue = -1;
        String line, newLine, productKey, userKey, score;
        String[] sLine;
        while ((line = br.readLine()) != null) {
            sLine = line.split(":");
            switch (sLine[0]) {
                case "product/productId":
                    productKey = sLine[1].trim();
                    productValue = productsMap.getOrDefault(productKey, -1);
                    if (productValue == -1) {
                        productValue = productIndex;
                        productsMap.put(productKey, productValue);
                        productIndex++;
                    }
                    break;
                case "review/userId":
                    userKey = sLine[1].trim();
                    userValue = usersMap.getOrDefault(userKey, -1);
                    if (userValue == -1) {
                        userValue = userIndex;
                        usersMap.put(userKey, userValue);
                        userIndex++;
                    }
                    break;
                case "review/score":
                    score = sLine[1].trim();
                    newLine = String.format("%d,%d,%s\n", userValue, productValue, score);
                    fw.write(newLine);
                    break;
            }
        }
        fw.close();
        br.close();
        writeMap(productsMap, productsFile);
        writeMap(usersMap, usersFile);
    }

    private void loadDataFromFile() throws IOException, ClassNotFoundException {
        this.productsMap = readMap(productsFile);
        this.usersMap = readMap(usersFile);
    }

    private void writeMap(HashMap map, File outputFile) throws IOException {
        try {
            FileOutputStream fos = new FileOutputStream(outputFile);
            ObjectOutputStream oos = new ObjectOutputStream(fos);
            oos.writeObject(map);
            oos.flush();
            oos.close();
            fos.close();
        } catch (Exception e) {
            System.out.println("Something went wrong writing the map output file!");
        }
    }

    private HashMap<String, Integer> readMap(File inputFile) throws IOException, ClassNotFoundException {
        FileInputStream fis = new FileInputStream(inputFile);
        ObjectInputStream ois = new ObjectInputStream(fis);
        HashMap<String, Integer> map = (HashMap<String, Integer>) ois.readObject();
        ois.close();
        fis.close();
        return map;
    }
}