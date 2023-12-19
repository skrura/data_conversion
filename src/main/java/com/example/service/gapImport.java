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
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.mongodb.WriteConcern.ACKNOWLEDGED;
import static java.lang.Math.floor;

@Service
public class gapImport {

    @Autowired
    private Environment environment;

    @Autowired(required = false)
    private MongoTemplate mongoTemplate;


    @Value("${spring.data.mongodb.port:}")
    private String port;
    @Value("${spring.data.mongodb.host:}")
    private String host;

    private String outGapThreadDispatch(List<String> socialCard, int theadNum, String collectionName, int dataSize) {
        CountDownLatch latch = new CountDownLatch(theadNum);
        List<List<String>> sCard = dataDistribution(socialCard, theadNum);

        // 固定线程数线程池
        ExecutorService executor = Executors.newFixedThreadPool(theadNum);
        for (int i = 0; i < sCard.size(); i++) {
            int finalI = i;
            executor.execute(() -> {
                outpatientGapImport(sCard.get(finalI), collectionName);
                latch.countDown();
            });
            while (!executor.isTerminated()) {
                // 等待所有任务完成
                latch.countDown();
            }
        }
        return "complete";
    }

    private String stayGapThreadDispatch(List<String> socialCard, int theadNum, String collectionName, int dataSize) {
        CountDownLatch latch = new CountDownLatch(theadNum);
        List<List<String>> sCard = dataDistribution(socialCard, theadNum);

        // 固定线程数线程池
        ExecutorService executor = Executors.newFixedThreadPool(theadNum);
        for (int i = 0; i < sCard.size(); i++) {
            int finalI = i;
            executor.execute(() -> {
                stayGapImport(sCard.get(finalI), collectionName);
                latch.countDown();
            });
            while (!executor.isTerminated()) {
                // 等待所有任务完成
                latch.countDown();
            }
        }
        return "complete";
    }

    private void stayGapImport(List<String> socialCard, String collectionName) {
        String mingxi = environment.getProperty("mingxiku");
        String jiesuan = environment.getProperty("jiesuanku");
        String zhenduan = environment.getProperty("zhenduanku");

        String connectionString = "mongodb://" + this.host + ":" + this.port;
        // 构建 MongoClientSettings
        MongoClientSettings settings;
        settings = MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(connectionString))
                .writeConcern(ACKNOWLEDGED)
                .build();

        InsertManyOptions options = new InsertManyOptions()
                .bypassDocumentValidation(false)
                .ordered(false);

        //开启MongoDB
        String dbName = "ns-saas";
        try (MongoClient mongoClient = MongoClients.create(settings)) {
            MongoDatabase database = mongoClient.getDatabase(dbName);
            MongoCollection<Document> collection = database.getCollection(collectionName);

            // 初始条件（大前提）
            Criteria criteria = Criteria.where("benefit_type").in("本地职工", "本地居保", "省内异地", "省外异地")
                    .andOperator(Criteria.where("medical_mode").is("住院"));
            List<Document> batchDocuments = new ArrayList<>();
            for (String s : socialCard) {
                Query query = Query.query(Criteria.where("social_card").is(s).andOperator(criteria)).with(Sort.by(Sort.Direction.ASC, "in_date"));
                query.fields().include("card_id","social_card","age",
                        "patient_name","sex","birthday","company_name",
                        "area_name_person","benefit_type","medical_mode",
                        "medical_name","medical_type","medical_grade",
                        "medical_nature","in_date","out_date","hospital_num").exclude("_id");

                List<Map> record = mongoTemplate.find(query, Map.class, jiesuan);
                //单条容器
                Document doc = new Document();
                if (record.size() <= 1) {
                    continue;
                }
                //是否异天标志
                Boolean diffDay = true;
                Long before = 0L;

                for (int i = 1; i < record.size(); i++) {
                    Query zquery = Query.query(Criteria.where("eposide_id").is(record.get(i).get("eposide_id")).andOperator(criteria));
                    zquery.fields().include("main_flag","diag_name").exclude("_id");
                    Map zrecord = mongoTemplate.findOne(zquery, Map.class, zhenduan);
                    if (zrecord != null && zrecord.isEmpty()) {
                        record.get(i).put("main_flag", zrecord.get("main_flag"));
                        record.get(i).put("diag_name", zrecord.get("diag_name"));
                    } else {
                        record.get(i).put("main_flag", "");
                        record.get(i).put("diag_name", "");
                    }
                }
                for (int i = 1; i < record.size(); i++) {
                    // 系统字段
                    String mainId = UUID.randomUUID().toString();
                    doc.put("_id", mainId);
                    doc.put("create_time", System.currentTimeMillis());
                    doc.put("create_account", "admin");
                    doc.put("category_id", "ed8eb695-2604-453e-a767-99fca467a898");
                    doc.put("data_status", "已归档");
                    doc.put("data_type", 1);
                    doc.put("priority", "");
                    doc.put("bind_id", mainId);
                    doc.put("corp_id", "nsrcu88p7uy22m7i9ioz");
                    doc.put("parent_corp_id_list", new ArrayList<>());
                    doc.put("bind_category_id", "");
                    //
                }
            }
        }
    }

    private void outpatientGapImport(List<String> socialCard, String collectionName) {

        String mingxi = environment.getProperty("mingxiku");
        String jiesuan = environment.getProperty("jiesuanku");
        String zhenduan = environment.getProperty("zhenduanku");

        String connectionString = "mongodb://" + this.host + ":" + this.port;
        // 构建 MongoClientSettings
        MongoClientSettings settings;
        settings = MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(connectionString))
                .writeConcern(ACKNOWLEDGED)
                .build();

        InsertManyOptions options = new InsertManyOptions()
                .bypassDocumentValidation(false)
                .ordered(false);
       /* // 根据ipone构建one
        String connectionStringOne = "mongodb://" + ipOne;
        // 构建 MongoClientSettings
        MongoClientSettings settingsOne;
        settings = MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(connectionStringOne))
                .writeConcern(ACKNOWLEDGED)
                .build();

        // 根据iptwo构建two
        String connectionStringTwo = "mongodb://" + ipTwo;
        // 构建 MongoClientSettings
        MongoClientSettings settingsTwo;
        settings = MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(connectionStringTwo))
                .writeConcern(ACKNOWLEDGED)
                .build();

        // 根据ipthree构建two
        String connectionStringThree = "mongodb://" + ipThree;
        // 构建 MongoClientSettings
        MongoClientSettings settingsThree;
        settings = MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(connectionStringThree))
                .writeConcern(ACKNOWLEDGED)
                .build();*/

        //开启MongoDB
        String dbName = "ns-saas";
        try (MongoClient mongoClient = MongoClients.create(settings)) {
            MongoDatabase database = mongoClient.getDatabase(dbName);
            MongoCollection<Document> collection = database.getCollection(collectionName);

            // 初始条件（大前提）
            Criteria criteria = Criteria.where("benefit_type").in("本地职工", "本地居保", "省内异地", "省外异地")
                    .andOperator(Criteria.where("medical_mode").in("普通门诊", "门慢", "门特", "药店购药"));

            List<Document> batchDocuments = new ArrayList<>();
            for (String s : socialCard) {
                Query query = Query.query(Criteria.where("social_card").is(s).andOperator(criteria)).with(Sort.by(Sort.Direction.ASC, "cost_time"));
                List<Map> record = mongoTemplate.find(query, Map.class, mingxi);
                //单条容器
                Document doc = new Document();
                if (record.size() <= 1) {
                    continue;
                }
                Query jquery = Query.query(Criteria.where("social_card").is(s).andOperator(criteria)).with(Sort.by(Sort.Direction.ASC, "in_date"));
                List<Map> jierecord = mongoTemplate.find(jquery, Map.class, jiesuan);
                //是否异天标志
                Boolean diffDay = true;
                Long before = 0L;
                //联表查询字段塞入明细记录中
                for (int i = 1; i < record.size(); i++) {
                    if (!jierecord.isEmpty()) {
                        record.get(i).put("age", jierecord.get(0).get("age"));
                        record.get(i).put("patient_name", jierecord.get(0).get("patient_name"));
                        record.get(i).put("sex", jierecord.get(0).get("sex"));
                        record.get(i).put("birthday", jierecord.get(0).get("birthday"));
                        record.get(i).put("area_name_person", jierecord.get(0).get("area_name_person"));
                        record.get(i).put("company_name", jierecord.get(0).get("company_name"));
                        if (!jierecord.get(i).isEmpty()) {
                            record.get(i).put("medical_type", jierecord.get(i).get("medical_type"));
                            record.get(i).put("medical_grade", jierecord.get(i).get("medical_grade"));
                            record.get(i).put("medical_nature", jierecord.get(i).get("medical_nature"));
                        }
                    } else {
                        record.get(i).put("age", "");
                        record.get(i).put("patient_name", "");
                        record.get(i).put("sex", "");
                        record.get(i).put("birthday", "");
                        record.get(i).put("area_name_person", "");
                        record.get(i).put("company_name", "");
                        record.get(i).put("medical_type", "");
                        record.get(i).put("medical_grade", "");
                        record.get(i).put("medical_nature", "");
                    }
                }
                for (int i = 1; i < record.size(); i++) {
                    // 系统字段
                    String mainId = UUID.randomUUID().toString();
                    doc.put("_id", mainId);
                    doc.put("create_time", System.currentTimeMillis());
                    doc.put("create_account", "admin");
                    doc.put("category_id", "ed8eb695-2604-453e-a767-99fca467a898");
                    doc.put("data_status", "已归档");
                    doc.put("data_type", 1);
                    doc.put("priority", "");
                    doc.put("bind_id", mainId);
                    doc.put("corp_id", "nsrcu88p7uy22m7i9ioz");
                    doc.put("parent_corp_id_list", new ArrayList<>());
                    doc.put("bind_category_id", "");
                    // 业务字段
                    doc = outpatientGapBusinessField(record.get(i), doc);
                    // 计算字段
                    // 门诊间隔天数
                    if (diffDay) {
                        before = Long.parseLong(record.get(i - 1).get("cost_time").toString());
                        Long after = Long.parseLong(record.get(i).get("cost_time").toString());
                        Double day = ((double) before - after) / (1000 * 3600 * 24);
                        if (day < 1) {
                            diffDay = false;
                        } else {
                            doc.put("interval_day", pack("天", day));
                            diffDay = true;
                        }
                    } else {
                        Long after = Long.parseLong(record.get(i).get("cost_time").toString());
                        Double day = ((double) before - after) / (1000 * 3600 * 24);
                        if (day < 1) {
                            diffDay = false;
                        } else {
                            doc.put("interval_day", pack("天", day));
                            diffDay = true;
                        }
                    }
                    // 年就诊次数
                    doc.put("eposide_number", record.size());
                    Set<String> drugType = new HashSet<>();
                    Double drugMoney = 0.0;
                    for (Map map : record) {
                        if (map.get("charge_type").toString().contains("药")) {
                            drugType.add((String) map.get("charge_type"));
                            drugMoney += Double.parseDouble(map.get("money").toString());
                        }
                    }
                    // 药品种类数量
                    doc.put("drug_number", pack("次", drugType.size()));
                    // 药品总金额
                    doc.put("drug_money", pack("元", drugMoney));
                    // 医院数量
                    Set<String> hpType = new HashSet<>();
                    for (Map map : record) {
                        hpType.add(map.get("medical_code").toString());
                    }
                    doc.put("hospital_number", hpType.size());

                    batchDocuments.add(doc);
                    //满5000入库
                    if (batchDocuments.size() >= Integer.parseInt(environment.getProperty("putIn"))) {
                        collection.insertMany(batchDocuments, options);
                        batchDocuments.clear();
                    }
                }
            }
            //剩余入库
            if (batchDocuments.size() > 0) {
                collection.insertMany(batchDocuments, options);
                batchDocuments.clear();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 住院间隔业务字段
     *
     * @param map
     * @param doc
     * @return
     */
    private Document stayGapBusinessField(Map map, Document doc) {
//            患者证件号码
        doc.put("card_id", map.get("card_id"));
//            患者社会保障卡号
        doc.put("social_card", map.get("social_card"));
//            患者年龄
        Map<String, Object> age = pack("岁", map.get("age"));
        doc.put("age", age);
//            患者姓名
        doc.put("patient_name", map.get("patient_name"));
//            患者性别
        doc.put("sex", map.get("sex"));
//            患者出生日期
        doc.put("birthday", map.get("birthday"));
//            患者工作单位
        doc.put("company_name", map.get("company_name"));
//            参保人统筹区名称
        doc.put("area_name_person", map.get("area_name_person"));
//            险种类型
        doc.put("benefit_type", map.get("benefit_type"));
//            医疗类别
        doc.put("medical_mode", map.get("medical_mode"));
//            医院名称
        doc.put("medical_name", map.get("medical_name"));
//            医院类别
        doc.put("medical_type", map.get("medical_type"));
//            医院等级
        doc.put("medical_grade", map.get("medical_grade"));
//            医院性质
        doc.put("medical_nature", map.get("medical_nature"));
//            科室名称
        doc.put("diag_dept", map.get("in_date"));
//            执行科室名称
//            主诊疾病名称
//            总金额
        Map<String, Object> total = pack("元", map.get(""));
        doc.put("money_total", total);
//            医保范围费用
        Map<String, Object> health = pack("元", map.get(""));
        doc.put("money_medical", health);

        return doc;
    }

    /**
     * 门诊间隔业务字段
     *
     * @param map
     * @param doc
     * @return
     */
    private Document outpatientGapBusinessField(Map map, Document doc) {
//        患者证件号码
        doc.put("card_id", map.get("card_id"));
//        患者社会保障卡号
        doc.put("social_card", map.get("social_card"));
//        患者年龄
        doc.put("social_card", map.get("age"));
//        患者姓名
        doc.put("social_card", map.get("patient_name"));
//        患者性别
        doc.put("social_card", map.get("sex"));
//        患者出生日期
        doc.put("social_card", map.get("birthday"));
//        患者工作单位
        doc.put("social_card", map.get("company_name"));
//        患者住址
        doc.put("address", "");
//        参保人统筹区名称
        doc.put("social_card", map.get("area_name_person"));
//        险种类型
        doc.put("benefit_type", map.get("benefit_type"));
//        医疗类别
        doc.put("medical_mode", map.get("medical_mode"));
//        医院名称
        doc.put("medical_name", map.get("medical_name"));
//        医院类别
        doc.put("social_card", map.get("medical_type"));
//        医院等级
        doc.put("social_card", map.get("medical_grade"));
//        医院性质
        doc.put("social_card", map.get("medical_nature"));
//        科室名称
        doc.put("diag_dept", map.get("dept_name"));
//        执行科室名称
        doc.put("discharge_dept_name", map.get(""));
//        主诊疾病名称
        doc.put("diag_name", map.get("discharge_dept_name"));
//        总金额
        doc.put("money_total", map.get("money"));
//        医保范围费用
        doc.put("money_medical", map.get("money_medical"));

        return doc;
    }


    /**
     * 数据分发
     *
     * @param Data
     * @return
     */
    public List<List<String>> dataDistribution(List<String> Data, int theadNum) {
        List<List<String>> distributionData = new ArrayList<>();
        int size = Data.size();

        //计算各个线程可以整块分发的最大数据量
        int amountData = (int) floor(size / theadNum);
        List a = new ArrayList<>();
        int end = 0;
        for (Integer i = 0; i < theadNum; i++) {
            List<String> data = new ArrayList<>();
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

    /**
     * 包装量纲
     *
     * @param unit
     * @param value
     * @return
     */
    private Document pack(String unit, Object value) {
        Document map = new Document();
        map.put("unit", unit);
        map.put("value", value);
        return map;
    }
}
