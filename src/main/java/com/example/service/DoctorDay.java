package com.example.service;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.InsertManyOptions;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.mongodb.WriteConcern.ACKNOWLEDGED;
import static com.mongodb.WriteConcern.UNACKNOWLEDGED;
import static java.lang.Math.floor;

@Service
public class DoctorDay {
    @Autowired
    private Environment environment;

    @Autowired(required = false)
    private MongoTemplate mongoTemplate;


    @Value("${spring.data.mongodb.port:}")
    private String port;
    @Value("${spring.data.mongodb.host:}")
    private String host;

    private String dayThreadDispatch(List<Map> doCodeDay, int theadNum, String collectionName) {
        CountDownLatch latch = new CountDownLatch(theadNum);
        List<List<Map>> dCodeDay = dataDistribution(doCodeDay, theadNum);

        // 固定线程数线程池
        ExecutorService executor = Executors.newFixedThreadPool(theadNum);
        for (int i = 0; i < dCodeDay.size(); i++) {
            int finalI = i;
            executor.execute(() -> {
                doctorDayImport(dCodeDay.get(finalI), collectionName);
                latch.countDown();
            });
            while (!executor.isTerminated()) {
                // 等待所有任务完成
                latch.countDown();
            }
        }
        return "complete";
    }

    private void doctorDayImport(List<Map> docCodeDay, String collectionName) {
        // 明细库
        String detailsLibrary = environment.getProperty("detailsLibrary");
        // 结算库
        String settlementOfPaymentLibrary = environment.getProperty("settlementOfPaymentLibrary");
        // 诊疗库
        String diagnosisLibrary = environment.getProperty("diagnosisLibrary");

        String connectionString = "mongodb://" + this.host + ":" + this.port;
        // 构建 MongoClientSettings
        MongoClientSettings settings;
        settings = MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(connectionString))
                .writeConcern(UNACKNOWLEDGED)
                .build();

        InsertManyOptions options = new InsertManyOptions()
                .ordered(false);

        //开启MongoDB
        String dbName = "ns-saas";
        try (MongoClient mongoClient = MongoClients.create(settings)) {
            MongoDatabase database = mongoClient.getDatabase(dbName);
            MongoCollection<Document> collection = database.getCollection(collectionName);

            // 初始条件（大前提）
            Criteria criteria = Criteria.where("benefit_type").in("本地职工", "本地居保", "省内异地", "省外异地");
            List<Document> batchDocuments = new ArrayList<>();
            for (Map map : docCodeDay) {
                
            }
        }

    }
    /**
     * 数据分发
     *
     * @param Data
     * @return
     */
    public List<List<Map>> dataDistribution (List < Map > Data,int theadNum){
        List<List<Map>> distributionData = new ArrayList<>();
        int size = Data.size();

        //计算各个线程可以整块分发的最大数据量
        int amountData = (int) floor(size / theadNum);
        List a = new ArrayList<>();
        int end = 0;
        for (Integer i = 0; i < theadNum; i++) {
            List<Map> data = new ArrayList<>();
            int begin = (int) (i * amountData);
            end = (int) ((i + 1) * amountData);
            if (i == theadNum - 1)
                data = Data.subList(begin, size);
            else
                data = Data.subList(begin, end);
            distributionData.add(data);
        }
        return distributionData;
    }
}
