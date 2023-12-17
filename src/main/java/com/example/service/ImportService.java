package com.example.service;

import cn.hutool.core.io.FileUtil;
import cn.hutool.core.io.IoUtil;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.InsertManyOptions;
import com.opencsv.CSVReader;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.aggregation.TypedAggregation;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.stereotype.Service;


import java.io.*;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

import static com.mongodb.WriteConcern.ACKNOWLEDGED;
import static java.lang.Math.floor;


@Service
public class ImportService {

    @Autowired(required = false)
    private MongoTemplate mongoTemplate;
    private static int amountData = 0;

    @Value("${spring.data.mongodb.port:}")
    private String port;
    @Value("${spring.data.mongodb.host:}")
    private String host;

    private static int theadData = 500000;

    /**
     * 原始三张表
     *
     * @param collectionName
     */
    public String importsDataOpt(String collectionName, String filepath, int size, int theadnum) {
        filepath = "D:\\桌面\\第一人民门诊明细.csv";
        //转换数据方便数据分发
        Collection<List<String>> csvData = dataExtraction(filepath, size, theadnum);
        //数据转换
        List<List<String>> result = new ArrayList<>(csvData);
        //数据分发
        List<List<List<String>>> csvDataList = dataDistribution(result,theadnum);
        CountDownLatch latch = new CountDownLatch(8);
        //开启线程
        for (int i = 0; i < theadnum; i++) {
            int finalI = i;
            new Thread(() -> {
                //主要导库操作
                read(csvDataList.get(finalI), collectionName, "zhenduan", finalI);
                latch.countDown();
            }).start();
        }
        try {
            latch.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        return "complete";
    }


    /**
     * 间隔控制中心
     *
     * @param target
     * @param collectionName
     * @param theadnum
     * @return
     */
    public String gapControl(String target, String collectionName, int theadnum) {
        String jiesuan = "";
        String zhenduan = "";
        String mingxi = "";
        Criteria criteria = new Criteria();
        switch (target) {
            case "zhuyuanjiange":
                Criteria criteria0 = Criteria.where("benefit_type").in("本地职工", "本地居保", "省内异地", "省外异地");
                Criteria criteria1 = Criteria.where("medical_mode").is("住院");
                criteria = new Criteria().andOperator(criteria0, criteria1);
                break;
            case "menzhenjiange":
                Criteria criteria2 = Criteria.where("benefit_type").in("本地职工", "本地居保", "省内异地", "省外异地");
                Criteria criteria3 = Criteria.where("medical_mode").in("普通门诊", "门慢", "门特", "药店购药");
                criteria = new Criteria().andOperator(criteria2, criteria3);
        }

        for (int i = 0; i < theadnum; i++) {
            gapImport(criteria, target, jiesuan, zhenduan, mingxi, collectionName, i, theadData);
        }
        return "ok";
    }

    public String statisticsControl(String target, String collectionName, int theadnum) {
        String jiesuan = "";
        String zhenduan = "";
        String mingxi = "";
        Criteria criteria = new Criteria();
        switch (target) {
            case "zhenliaotongji":
                criteria = Criteria.where("benefit_type").in("本地职工", "本地居保", "省内异地", "省外异地");
                break;
        }
        for (int i = 0; i < theadnum; i++) {
            statisticsImport(criteria, target, jiesuan, zhenduan, mingxi, collectionName, i, theadData);
        }
        return "ok";
    }

    private void statisticsImport(Criteria criteria, String target, String jiesuan, String zhenduan, String mingxi, String collectionName, int theadId, int limit) {
        List<Map> mapList = new ArrayList<>();
        List<Map> charge = new ArrayList<>();
        switch (target) {
            case "zhenliaotongji":
                mapList = zStatisticsBuildingQuery(criteria, jiesuan, zhenduan, mingxi, theadId * theadData, limit).getMappedResults();
        }

    }

    /**
     * 住院与门诊间隔数据入库
     *
     * @param criteria
     * @param jiesuan
     * @param zhenduan
     * @param collectionName
     * @param theadId
     * @param limit
     */
    private void gapImport(Criteria criteria, String target, String jiesuan, String zhenduan, String mingxi, String collectionName, int theadId, int limit) {
        List<Map> mapList = new ArrayList<>();
        switch (target) {
            case "zhuyuanjiange":
                mapList = gapStayBuildingQuery(criteria, jiesuan, zhenduan, theadId * theadData, limit).getMappedResults();
                break;
            case "menzhenjiange":
                mapList = outpatientGapBuildingQuery(criteria, mingxi, zhenduan, jiesuan, theadId * theadData, limit).getMappedResults();
                break;
        }
        Map<String, List<Map>> gapMap = new HashMap<>();
        for (Map map : mapList) {
            String fieldValue = map.get("social_card").toString();
            if (gapMap.get(fieldValue) == null) {
                gapMap.put(fieldValue, new ArrayList<>());
            }
            gapMap.get(fieldValue).add(map);
        }

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

            int i = 1;
            Double gap = 0.0;
            Long skip = (long) (theadId * theadData);
            List<Document> batchDocuments = new ArrayList<>();
            for (Map.Entry<String, List<Map>> entry : gapMap.entrySet()) {
                List<Map> traversal = entry.getValue();
                for (int t = 1; i < traversal.size(); i++) {
                    Document doc = new Document();
                    //系统字段
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
                    switch (target) {
                        case "zhuyuanjiange":
                            //住院业务字段
                            doc = stayGapBusinessField(traversal.get(t), doc, t);
                            //住院计算字段    住院间隔天数
                            Long inDate = (Long) traversal.get(t).get("in_date");
                            Long outDate = (Long) traversal.get(t - 1).get("out_date");
                            Long time = inDate - outDate;
                            Double day = time / (1000 * 60 * 60 * 24.0);
                            Map daymap = pack("天", day);
                            doc.put("interval_day", daymap);
                            break;
                        case "menzhenjiange":
                            //门诊计算字段
                            // 门诊间隔天数
                            Long lastDate = (Long) traversal.get(t - 1).get("cost_time");
                            Long nextDate = (Long) traversal.get(t).get("cost_time");
                            gap += (nextDate - lastDate) / (1000 * 60 * 60 * 24.0);
                            if (gap > 1.00) {
                                gap = 0.0;
                                //业务字段非异天间隔跳过
                                doc = outpatientGapBusinessField(traversal.get(t), doc, t);
                                doc.put("interval_day", pack("天", gap));
                                //        年就诊次数
                                doc.put("eposide_number", traversal.size());
                                //计算字段
                                Set<String> drugKind = new HashSet<>();
                                Set<String> Hospital = new HashSet<>();
                                int drugMoney = 0;
                                for (Map map : traversal) {
                                    drugKind.add((String) map.get("item_code"));
                                    Hospital.add((String) map.get("medical_name"));
                                    if (map.get("charge_type").toString().contains("药")) {
                                        drugMoney += Integer.parseInt(map.get("money").toString());
                                    }
                                }
                                //        药品种类数量
                                doc.put("drug_number", pack("种", drugKind.size()));
                                //        药品总金额
                                doc.put("drug_money", pack("元", drugMoney));
                                //        医院数量
                                doc.put("hospital_number", pack("个", Hospital.size()));
                            }
                            break;
                    }
                    doc.put("zone", i + skip);
                    i++;
                    batchDocuments.add(doc);
                    if (batchDocuments.size() >= 5000) {
                        collection.insertMany(batchDocuments, options);
                        batchDocuments.clear();
                    }
                }
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
     * @param t
     * @return
     */
    private Document stayGapBusinessField(Map map, Document doc, int t) {
        ArrayList diagDept = new ArrayList<>();
        ArrayList mainFlag = new ArrayList<>();
        ArrayList diagName = new ArrayList<>();
        if (map.get("diag_dept") != null)
            diagDept = (ArrayList) map.get("diag_dept");
        if (map.get("main_flag") != null)
            mainFlag = (ArrayList) map.get("main_flag");
        if (map.get("diag_name") != null)
            diagName = (ArrayList) map.get("diag_name");
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
        if (diagDept.size() > t) {
            doc.put("discharge_dept_name", diagDept.get(t));
        } else {
            doc.put("discharge_dept_name", diagDept.get(0));
        }
        //            主诊疾病名称
        if (mainFlag.size() > t) {
            if (mainFlag.get(t).equals("1")) {
                if (diagName.size() > t) {
                    doc.put("diag_name", diagName.get(t));
                } else {
                    doc.put("diag_name", diagName.get(0));
                }
            } else {
                doc.put("diag_name", "");
            }
        }
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
     * @param t
     * @return
     */
    private Document outpatientGapBusinessField(Map map, Document doc, int t) {
        //随t变化
        ArrayList diagDept = new ArrayList<>();
        ArrayList mainFlag = new ArrayList<>();
        ArrayList diagName = new ArrayList<>();
        ArrayList medicalType = new ArrayList<>();
        ArrayList medicalGrade = new ArrayList<>();
        ArrayList medicalNature = new ArrayList<>();
        if (map.get("diag_dept") != null)
            diagDept = (ArrayList) map.get("diag_dept");
        if (map.get("main_flag") != null)
            mainFlag = (ArrayList) map.get("main_flag");
        if (map.get("diag_name") != null)
            diagName = (ArrayList) map.get("diag_name");
        if (map.get("medical_type") != null)
            medicalType = (ArrayList) map.get("medical_type");
        if (map.get("medical_grade") != null)
            medicalGrade = (ArrayList) map.get("medical_grade");
        if (map.get("medical_nature") != null)
            medicalNature = (ArrayList) map.get("medical_nature");
        //不随t变化
        ArrayList birthday = new ArrayList<>();
        ArrayList age = new ArrayList<>();
        ArrayList sex = new ArrayList<>();
        ArrayList companyName = new ArrayList<>();
        ArrayList areaNamePerson = new ArrayList<>();
        if (map.get("birthday") != null)
            birthday = (ArrayList) map.get("birthday");
        if (map.get("age") != null)
            age = (ArrayList) map.get("age");
        if (map.get("sex") != null)
            sex = (ArrayList) map.get("sex");
        if (map.get("company_name") != null)
            companyName = (ArrayList) map.get("company_name");
        if (map.get("area_name_person") != null)
            areaNamePerson = (ArrayList) map.get("area_name_person");

//        患者证件号码
        doc.put("card_id", map.get("card_id"));
//        患者社会保障卡号
        doc.put("social_card", map.get("social_card"));
//        患者年龄
        if (!age.isEmpty()) {
            Map<String, Object> aGe = pack("岁", age.get(0));
            doc.put("age", aGe);
        }
//        患者姓名
        doc.put("patient_name", map.get("patient_name"));
//        患者性别
        if (!sex.isEmpty()) {
            doc.put("sex", sex.get(0));
        }
//        患者出生日期
        if (!birthday.isEmpty()) {
            doc.put("birthday", birthday.get(0));
        }
//        患者工作单位
        if (!companyName.isEmpty()) {
            doc.put("company_name", companyName.get(0));
        }
//        患者住址
        doc.put("address", "");
//        参保人统筹区名称
        if (!areaNamePerson.isEmpty())
            doc.put("area_name_person", areaNamePerson.get(0));
//        险种类型
        doc.put("benefit_type", map.get("benefit_type"));
//        医疗类别
        doc.put("medical_mode", map.get("medical_mode"));
//        医院名称
        doc.put("medical_name", map.get("medical_name"));
//        医院类别
        if (!medicalType.isEmpty()) {
            if (medicalType.size() > t) {
                doc.put("medical_type", medicalType.get(t));
            } else {
                doc.put("medical_type", medicalType.get(0));
            }
        }
//        医院等级
        if (!medicalGrade.isEmpty()) {
            if (medicalGrade.size() > t) {
                doc.put("medical_grade", medicalGrade.get(t));
            } else {
                doc.put("medical_grade", medicalGrade.get(0));
            }
        }
//        医院性质
        if (!medicalNature.isEmpty()) {
            if (medicalNature.size() > t) {
                doc.put("medical_nature", medicalNature.get(t));
            } else {
                doc.put("medical_nature", medicalNature.get(0));
            }
        }
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


    private AggregationResults<Map> zStatisticsBuildingQuery(Criteria criteria, String mingxi, String zhenduan, String jiesuan, int skip, int limit) {
        TypedAggregation<Map> TypedAggregation = Aggregation.newAggregation(
                Map.class,
                Aggregation.limit(limit),
                Aggregation.skip(skip),
                Aggregation.sort(Sort.by(Sort.Order.asc("social_card"))).and(Sort.by(Sort.Order.asc("cost_time"))),
                Aggregation.match(criteria),
                Aggregation.group("social_card", "charge_type").count().as("count").sum("money").as("money")
                        .first("medical_name").as("medical_name").first("card_id").as("card_id")
                        .first("patient_name").as("patient_name").first("benefit_type").as("benefit_type")
                        .first("medical_mode").as("medical_mode").first("eposide_id").as("eposide_id")
                        .first("dept_name").as("dept_name").first("discharge_dept_name").as("discharge_dept_name")
                        .first("doctor_code").as("doctor_code").first("doctor_name").as("doctor_name"),
                Aggregation.lookup(zhenduan, "eposide_id", "eposide_id", "zhenduan"),
                Aggregation.lookup(jiesuan, "social_card", "social_card", "jiesuan"),
                Aggregation.project("medical_name", "card_id", "patient_name", "benefit_type", "medical_mode", "eposide_id", "dept_name",
                        "discharge_dept_name", "doctor_code", "doctor_name", "zhenduan.main_flag", "zhenduan.diag_name",
                        "jiesuan.area_name_person", "jiesuan.medical_type", "jiesuan.medical_grade", "jiesuan.medical_nature",
                        "jiesuan.birthday", "jiesuan.age", "jiesuan.sex", "jiesuan.company_name", "jiesuan.money_total", "jiesuan.money_medical")
        ).withOptions(Aggregation.newAggregationOptions().allowDiskUse(true).build());

        AggregationResults<Map> aggregationResults0 = mongoTemplate.aggregate(
                TypedAggregation,
                mingxi,
                Map.class);
        return aggregationResults0;
    }

    /**
     * 构建门诊间隔查询条件
     *
     * @param criteria
     * @param mingxi
     * @param zhenduan
     * @param jiesuan
     * @param skip
     * @param limit
     * @return
     */
    private AggregationResults<Map> outpatientGapBuildingQuery(Criteria criteria, String mingxi, String zhenduan, String jiesuan, int skip, int limit) {
        TypedAggregation<Map> TypedAggregation = Aggregation.newAggregation(
                Map.class,
                Aggregation.limit(limit),
                Aggregation.skip(skip),
                Aggregation.sort(Sort.by(Sort.Order.asc("social_card"))).and(Sort.by(Sort.Order.asc("cost_time"))),
                Aggregation.match(criteria),
                Aggregation.lookup(zhenduan, "eposide_id", "eposide_id", "zhenduan"),
                Aggregation.lookup(jiesuan, "social_card", "social_card", "jiesuan"),
                Aggregation.project("area_person_code", "medical_name", "social_card", "card_id", "patient_name",
                        "benefit_type", "medical_mode", "cost_time", "money", "money_medical", "dept_name", "item_code",
                        "charge_type", "discharge_dept_name", "jiesuan.medical_type", "jiesuan.medical_grade", "jiesuan.medical_nature",
                        "jiesuan.birthday", "jiesuan.age", "jiesuan.sex", "jiesuan.company_name", "jiesuan.area_name_person",
                        "eposide_id", "zhenduan.main_flag", "zhenduan.diag_name", "zhenduan.diag_dept")
        ).withOptions(Aggregation.newAggregationOptions().allowDiskUse(true).build());

        AggregationResults<Map> aggregationResults0 = mongoTemplate.aggregate(
                TypedAggregation,
                mingxi,
                Map.class);
        return aggregationResults0;
    }

    /**
     * 住院间隔构建查询条件
     *
     * @param criteria
     * @param table
     * @return
     */
    private AggregationResults<Map> gapStayBuildingQuery(Criteria criteria, String table, String table1, int skip, int limit) {
        TypedAggregation<Map> TypedAggregation = Aggregation.newAggregation(
                Map.class,
                Aggregation.limit(limit),
                Aggregation.skip(skip),
                Aggregation.group("social_card"),
                Aggregation.match(criteria),
                Aggregation.lookup(table1, "eposide_id", "eposide_id", "zhenduan"),
                Aggregation.project("area_name", "area_name_person", "medical_name",
                        "medical_type", "medical_grade", "medical_nature", "card_id",
                        "patient_name", "birthday", "age", "sex", "company_name",
                        "benefit_type", "medical_mode", "money_total", "money_medical",
                        "in_date", "out_date", "dept_name", "eposide_id", "zhenduan.main_flag",
                        "zhenduan.diag_name", "zhenduan.diag_dept")
        ).withOptions(Aggregation.newAggregationOptions().allowDiskUse(true).build());

        AggregationResults<Map> aggregationResults0 = mongoTemplate.aggregate(
                TypedAggregation,
                table,
                Map.class);
        return aggregationResults0;
    }

    /**
     * @param filepath
     * @param size
     * @return
     */
    private Collection<List<String>> dataExtraction(String filepath, int size, int theadNum) {
        try {
            String[] line;
            Collection<List<String>> list = Collections.synchronizedCollection(Arrays.asList());
            List<String> joint = new ArrayList<>();
            File touch = FileUtil.touch(filepath);
            FileInputStream fileInputStream = IoUtil.toStream(touch);
            BufferedReader utf8Reader = IoUtil.getUtf8Reader(fileInputStream);
            Stream<String> lines = utf8Reader.lines();
            Integer jointLen = 0;
            lines.parallel().forEach(s -> {
                try (CSVReader csvReader = new CSVReader(new StringReader(s))) {
                    addLine(list, csvReader.readNext(), joint, size);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
            return list;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     *  每行加入List 逻辑
     * @param list
     * @param line
     * @param joint
     * @param size
     */
    void addLine(Collection<List<String>> list, String[] line, List<String> joint, int size) {
        List<String> linel = new ArrayList<>(Arrays.asList(line));
        joint.addAll(linel);
        if (joint.size()==size){
            List<String> jointCopy = new ArrayList<>(joint); // 创建joint的副本
            joint.clear();
            list.add(jointCopy);
        }
    }


    /**
     * csv数据分发
     *
     * @param csvData
     * @return
     */
    public List<List<List<String>>> dataDistribution(List<List<String>> csvData,int theadNum) {
        List<List<List<String>>> distributionData = new ArrayList<>();
        int size = csvData.size();

        //计算各个线程可以整块分发的最大数据量
        amountData = (int) floor(size / theadNum);
        List a = new ArrayList<>();
        int end = 0;
        for (Integer i = 0; i < theadNum; i++) {
            List<List<String>> data = new ArrayList<>();
            int begin = (int) (i * amountData);
            end = (int) ((i + 1) * amountData);
            if (i == theadNum-1)
                data = csvData.subList(begin, size);
            else
                data = csvData.subList(begin, end);
            distributionData.add(data);
        }
        return distributionData;
    }

    /**
     * 开启mongodb读取csv
     *
     * @param csvData
     * @param collectionName
     * @param target
     * @param theadId
     */
    public void read(List<List<String>> csvData, String collectionName, String target, int theadId) {

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
        List<Document> batchDocuments = new ArrayList<>();
        //开启MongoDB
        String dbName = "ns-saas";
        try (MongoClient mongoClient = MongoClients.create(settings)) {
            MongoDatabase database = mongoClient.getDatabase(dbName);
            MongoCollection<Document> collection = database.getCollection(collectionName);

            int i = 1;
            Long skip = (long) (theadId * amountData);
            for (List<String> csvDatum : csvData) {
                Document doc = new Document();
                //系统字段
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

                //业务字段
                switch (target) {
                    case "zhenduan":
                        doc = zhenduanfield(csvDatum, doc);
                        break;
                    case "jiesuan":
                        doc = jiesuanfield(csvDatum, doc);
                        break;
                    case "mingxi":
                        doc = mingxifield(csvDatum, doc);
                }
                //zone
                doc.put("zone", i + skip);
                i++;
                batchDocuments.add(doc);
                if (batchDocuments.size() >= 5000) {
                    collection.insertMany(batchDocuments, options);
                    batchDocuments.clear();
                }
            }
            // 插入剩余的数据
            if (!batchDocuments.isEmpty()) {
                collection.insertMany(batchDocuments, options);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 诊断字段
     *
     * @param list
     * @param doc
     * @return
     */
    private Document zhenduanfield(List<String> list, Document doc) {
        //就诊号
        doc.put("eposide_id", list.get(0));
        //出入院诊断类别
        doc.put("inout_diag_type", list.get(1));
        //诊断类别
        doc.put("diag_type", list.get(2));
        //是否为主诊
        doc.put("main_flag", list.get(3));
        //诊断代码
        doc.put("diag_code", list.get(4));
        //诊断名称
        doc.put("diag_name", list.get(5));
        //入院病情
        doc.put("adm_cond", list.get(6));
        //诊断科室
        doc.put("diag_dept", list.get(7));
        //诊断医师代码
        doc.put("diag_dr_code", list.get(8));
        //诊断医师姓名
        doc.put("diag_dr_name", list.get(9));
        //诊断时间
        doc.put("diag_time", list.get(10));
        return doc;
    }

    /**
     * 结算字段
     *
     * @param list
     * @param doc
     * @return
     */
    private Document jiesuanfield(List<String> list, Document doc) {
        //结算单据号
        doc.put("bill_id", list.get(0));
        //统筹区名称
        doc.put("area_name", list.get(1));
        //参保人统筹区名称
        doc.put("area_name_person", list.get(2));
        //医保年度
        doc.put("year", list.get(3));
        //定点机构编码
        doc.put("medical_code", list.get(4));
        //定点机构名称
        doc.put("medical_name", list.get(5));
        //定点机构类别
        doc.put("medical_type", list.get(6));
        //定点机构等级
        doc.put("medical_grade", list.get(7));
        //定点机构性质
        doc.put("medical_nature", list.get(8));
        //社会保障卡号
        doc.put("social_card", list.get(9));
        //患者证件号码
        doc.put("card_id", list.get(10));
        //患者姓名
        doc.put("patient_name", list.get(11));
        //患者出生日期
        doc.put("birthday", list.get(12));
        //患者年龄
        Document age = pack("岁", list.get(13));
        doc.put("age", age);
        //性别
        doc.put("sex", list.get(14));
        //单位名称
        doc.put("company_name", list.get(15));
        //险种类型
        doc.put("benefit_type", list.get(16));
        //医疗类别
        doc.put("medical_mode", list.get(17));
        //就诊号
        doc.put("eposide_id", list.get(18));
        //费用结算时间
        doc.put("clear_time", list.get(19));
        //医疗费总额
        Document moneyTotal = pack("元", list.get(20));
        doc.put("money_total", moneyTotal);
        //医保范围费用
        Document moneyMedical = pack("元", list.get(21));
        doc.put("money_medical", moneyMedical);
        //特殊病种标识
        doc.put("is_special", list.get(22));
        //入院日期
        doc.put("in_date", list.get(23));
        //出院日期
        doc.put("out_date", list.get(24));
        //住院天数
        Document hospitalNum = pack("天", list.get(25));
        doc.put("hospital_num", hospitalNum);
        //入院诊断疾病编码
        doc.put("in_diagnose_code", list.get(27));
        //入院疾病名称
        doc.put("in_diagnose_name", list.get(28));
        //出院疾病诊断编码
        doc.put("out_diagnose_code", list.get(29));
        //出院疾病名称
        doc.put("out_diagnose_name", list.get(30));
        //入院科室名称
        doc.put("dept_name", list.get(31));
        //出院科室名称
        doc.put("out_dept_name", list.get(32));
        //主治医生代码
        doc.put("doctorcode", list.get(33));
        //主治医生
        doc.put("doctorname", list.get(34));
        //离院方式
        doc.put("discharge_kind", list.get(35));
        //异地就诊标志
        doc.put("nonlocal_org_sign", list.get(36));
        return doc;
    }

    /**
     * 明细字段
     *
     * @param list
     * @param doc
     * @return
     */
    private Document mingxifield(List<String> list, Document doc) {
//        统筹区划代码
        doc.put("area_code", list.get(0));
//        参保人统筹区代码
        doc.put("area_person_code", list.get(1));
//        定点机构编码
        doc.put("medical_code", list.get(2));
//        定点机构名称
        doc.put("medical_name", list.get(3));
//        社会保障卡号
        doc.put("social_card", list.get(4));
//        证件号码
        doc.put("card_id", list.get(5));
//        人员编号
        doc.put("psn_no", list.get(6));
//        姓名
        doc.put("patient_name", list.get(7));
//        险种类型
        doc.put("benefit_type", list.get(8));
//        医疗类别
        doc.put("medical_mode", list.get(9));
//        就诊号
        doc.put("eposide_id", list.get(10));
//        单据号
        doc.put("bill_id", list.get(11));
//        单据明细号
        doc.put("bill_detail_id", list.get(12));
//         门诊或住院号
        doc.put("hospital_id", list.get(13));
//        费用发生时间
        doc.put("cost_time", list.get(14));
//        费用结算时间
        doc.put("clear_time", list.get(15));
//        医保目录编码
        doc.put("item_code", list.get(16));
//        医保目录名称
        doc.put("item_name", list.get(17));
//        机构收费项目编码
        doc.put("item_code_hosp", list.get(18));
//        机构收费项目名称
        doc.put("item_name_hosp", list.get(19));
//        收费项目类别
        doc.put("charge_type", list.get(20));
//         费用类别
        doc.put("cost_type", list.get(21));
//        单价
        Document unitPrice = pack("元", list.get(22));
        doc.put("unit_price", unitPrice);
//        限价
        Document maxPrice = pack("元", list.get(23));
        doc.put("max_price", maxPrice);
//        帖数
        Document dose = pack("个", list.get(24));
        doc.put("dose", dose);
//        数量
        Document num = pack("个", list.get(25));
        doc.put("num", num);
//        金额
        Document money = pack("元", list.get(26));
        doc.put("money", list.get(26));
//        自付比例
        Document payPerRetio = pack("比", list.get(27));
        doc.put("pay_per_retio", payPerRetio);
//        医保范围费用
        Document moneyMedical = pack("元", list.get(28));
        doc.put("money_medical", moneyMedical);
//        自理费用
        Document moneySelfPay = pack("元", list.get(29));
        doc.put("money_self_pay", moneySelfPay);
//        自费费用
        Document moneySelfOut = pack("元", list.get(30));
        doc.put("money_self_out", moneySelfOut);
//        剂型
        doc.put("dosage_form", list.get(31));
//        规格
        doc.put("spec", list.get(32));
//        药品剂型单位
        doc.put("pack_unit", list.get(33));
//        生产企业
        doc.put("bus_produce", list.get(34));
//        药品包装转化比
        Document packRetio = pack("比", list.get(35));
        doc.put("pack_retio", packRetio);
//        特殊病种标识
        doc.put("is_special", list.get(36));
//        是否处方药
        doc.put("is_recipel", list.get(37));
//        单复方标志
        doc.put("is_single", list.get(38));
//        处方号
        doc.put("recipel_no", list.get(39));
//        科室名称
        doc.put("dept_name", list.get(40));
//        执行科室名称
        doc.put("discharge_dept_name", list.get(41));
//        医生编码
        doc.put("doctor_code", list.get(42));
//       医生姓名
        doc.put("doctor_name", list.get(43));

        return doc;
    }

    /**
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
