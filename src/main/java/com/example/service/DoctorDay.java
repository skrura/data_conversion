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
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.mongodb.WriteConcern.UNACKNOWLEDGED;
import static java.lang.Math.floor;

@Service
public class DoctorDay {

    @Autowired
    private Environment environment;

    @Autowired(required = false)
    private MongoTemplate mongoTemplate;

    @Value("${spring.data.mongodb.database}")
    private String dbName;
    @Value("${spring.data.mongodb.port}")
    private String port;
    @Value("${spring.data.mongodb.host:}")
    private String host;

    /**
     * 按日线程控制
     *
     * @param doctorCodeDay  医生编码与日期为一组的集合
     * @param theadNum       线程数
     * @param collectionName 入库名
     * @return 调用完成标识
     */
    private String dayThreadDispatch(List<Map> doctorCodeDay, int theadNum, String collectionName) {
        CountDownLatch latch = new CountDownLatch(theadNum);
        List<List<Map>> dCodeDay = dataDistribution(doctorCodeDay, theadNum);

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

    /**
     * @param doctorCodeDay  医生编码与日期为一组的集合
     * @param collectionName 入库名
     */
    private void doctorDayImport(List<Map> doctorCodeDay, String collectionName) {
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
        try (MongoClient mongoClient = MongoClients.create(settings)) {
            MongoDatabase database = mongoClient.getDatabase(dbName);
            MongoCollection<Document> collection = database.getCollection(collectionName);

            // 初始条件（大前提）
            Criteria criteria = Criteria.where("benefit_type").in("本地职工", "本地居保", "省内异地", "省外异地");
            List<Document> batchDocuments = new ArrayList<>();
            for (Map map : doctorCodeDay) {
                long time = (long) map.get("time");
                String name = (String) map.get("name");
                // 将时间戳转换为ZonedDateTime对象
                ZonedDateTime zdt = ZonedDateTime.ofInstant(Instant.ofEpochMilli(time), ZoneId.systemDefault());
                // 获取当天的开始时间（00:00）
                ZonedDateTime start = zdt.toLocalDate().atStartOfDay(zdt.getZone());
                long startOfDay = start.toInstant().toEpochMilli();
                // 获取当天的结束时间（23:59:59.999）
                ZonedDateTime end = start.plusDays(1).minusNanos(1);
                long endOfDay = end.toInstant().toEpochMilli();
                Criteria criteriatime = Criteria.where("cost_time").gte(startOfDay).lte(endOfDay);
                Query query = Query.query(Criteria.where("social_card").is(name)
                        .andOperator(criteria).andOperator(criteriatime));

            }
        }

    }

    /**
     * 数据分发
     *
     * @param Data
     * @return
     */
    public List<List<Map>> dataDistribution(List<Map> Data, int theadNum) {
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
