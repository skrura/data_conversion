package com.example.service;

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
import org.springframework.core.env.Environment;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.aggregation.TypedAggregation;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;


import java.io.*;
import java.util.*;
import java.util.concurrent.CountDownLatch;

import static com.mongodb.WriteConcern.UNACKNOWLEDGED;
import static java.lang.Math.ceil;


@Service
public class ImportService {

    @Autowired(required = false)
    private MongoTemplate mongoTemplate;

    @Value("${spring.data.mongodb.port:}")
    private String port;
    @Value("${spring.data.mongodb.host:}")
    private String host;

    @Autowired
    private Environment environment;

    /**
     * 原始三张表
     *
     * @param collectionName 入库名
     * @param filepath       文件位置
     * @param target         要入哪个库
     * @param zoneMin        zone范围（小）
     * @param zoneMax        zone范围（大）
     * @param columnSize     列数量
     * @param theadNum       线程数量
     * @return 执行完成标识
     */
    public String importsDataOpt(String collectionName, String filepath, String target, int zoneMin, int zoneMax, int columnSize, long rowsSize, int theadNum) {
        //filepath = "D:\\桌面\\第一人民门诊明细.csv";
        CountDownLatch latch = new CountDownLatch(theadNum);
        long readLineNum = (long) ceil((double) rowsSize / theadNum);

        //开启线程
        for (int i = 0; i < theadNum; i++) {
            int finalI = i;
            new Thread(() -> {
                readAndLoad(filepath, collectionName, target, zoneMin, zoneMax, columnSize, finalI * readLineNum);
                latch.countDown();
            }).start();
        }
        try {
            latch.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        return target + ":" + "complete";
    }

    /**
     * 大宽表控制中心
     *
     * @param collectionName 入库名
     * @param theadNum       线程数
     * @param dataSize       数据量
     * @return 执行完成标识
     */
    public String wideControl(String collectionName, int theadNum, int dataSize) {
        int theadDataNum = (int) (Math.ceil((double) dataSize / theadNum));
        CountDownLatch latch = new CountDownLatch(theadNum);

        //开启线程
        for (int i = 0; i < theadNum; i++) {
            int finalI = i;
            new Thread(() -> {
                wideImport(finalI * theadDataNum, theadDataNum, collectionName);
                latch.countDown();
            }).start();
        }
        try {
            latch.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        return "Wide Complete";
    }


    /**
     * 导入大宽表
     *
     * @param skip           跳过数
     * @param theadDataNum   分到的数据量
     * @param collectionName 入库名
     */
    private void wideImport(int skip, int theadDataNum, String collectionName) {
        // 明细库
        String detailsLibrary = environment.getProperty("detailsLibrary");
        // 结算库
        String settlementOfPaymentLibrary = environment.getProperty("settlementOfPaymentLibrary");
        // 诊疗库
        String diagnosisLibrary = environment.getProperty("diagnosisLibrary");

        // 初始条件（大前提）
        Criteria criteria = Criteria.where("benefit_type").in("本地职工", "本地居保", "省内异地", "省外异地");
        //根据单据明细号分组
        List<Map> mapList = wideBuildingQuery(criteria, detailsLibrary, skip, theadDataNum).getMappedResults();


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
            List<Document> batchDocuments = new ArrayList<>();

            for (Map map : mapList) {
                // 构建本地查询
                MongoTemplate mongoTemplatew = new MongoTemplate(MongoClients.create(settings), dbName);
                // 构建单据明细号查询条件
                Criteria criteria1 = Criteria.where("bill_detail_id").is(map.get("_id"));
                // 合并初始条件
                // 根据单据明细号查询明细表
                Map detailsMap = mongoTemplatew.findOne(Query.query(new Criteria().andOperator(criteria, criteria1)), Map.class, detailsLibrary);
                // 主要明细为空
                if (detailsMap==null||detailsMap.isEmpty()) {
                    continue;
                }
                // 构建单据号查询条件
                Criteria criteria2 = Criteria.where("bill_id").is(detailsMap.get("bill_id"));
                // 合并初始条件
                // 根据单据号查询结算表
                Map settlementOfPaymentMap = mongoTemplatew.findOne(Query.query(new Criteria().andOperator(criteria, criteria2)), Map.class, settlementOfPaymentLibrary);
                // 构建就诊号查询条件
                Criteria criteria3 = Criteria.where("eposide_id").is(detailsMap.get("eposide_id"));
                // 合并初始条件
                // 根据就诊号查询诊断表
                Map diagnosisMap = mongoTemplatew.findOne(Query.query(new Criteria().andOperator(criteria, criteria3)), Map.class, diagnosisLibrary);
                // 单条数据
                Document doc = new Document();
                // 系统字段
                doc.put("create_time", System.currentTimeMillis());
                doc.put("create_account", "admin");
                doc.put("category_id", "ed8eb695-2604-453e-a767-99fca467a898");
                doc.put("data_status", "已归档");
                doc.put("data_type", 1);
                doc.put("priority", "");
                doc.put("bind_id", "");
                doc.put("corp_id", environment.getProperty("corp_id"));
                doc.put("parent_corp_id_list", "");
                doc.put("bind_category_id", "");
                // 业务字段
                if (settlementOfPaymentMap != null) {
                    // 统筹区名称
                    doc.put("area_name", settlementOfPaymentMap.get("area_name"));
                    //参保人统筹区名称
                    doc.put("area_name_person", settlementOfPaymentMap.get("area_name_person"));
                    //医保年度
                    doc.put("year", settlementOfPaymentMap.get("year"));
                    //出生日期
                    doc.put("birthday", settlementOfPaymentMap.get("birthday"));
                    //年龄
                    doc.put("age", settlementOfPaymentMap.get("age"));
                    //性别
                    doc.put("sex", settlementOfPaymentMap.get("sex"));
                    //单位名称
                    doc.put("company_name", settlementOfPaymentMap.get("company_name"));
                    //医疗费总额
                    doc.put("money_total", settlementOfPaymentMap.get("money_total"));
                    //统筹支付金额
                    doc.put("money_bmi", settlementOfPaymentMap.get("money_medical"));
                    //入院日期
                    doc.put("in_date", settlementOfPaymentMap.get("in_date"));
                    //出院日期
                    doc.put("out_date", settlementOfPaymentMap.get("out_date"));
                    //住院天数
                    doc.put("hospital_num", settlementOfPaymentMap.get("hospital_num"));
                    //入院诊断疾病编码
                    doc.put("in_diagnose_code", settlementOfPaymentMap.get("in_diagnose_code"));
                    //入院疾病名称
                    doc.put("in_diagnose_name", settlementOfPaymentMap.get("in_diagnose_name"));
                    //出院疾病诊断编码
                    doc.put("out_diagnose_code", settlementOfPaymentMap.get("out_diagnose_code"));
                    //出院疾病名称
                    doc.put("out_diagnose_name", settlementOfPaymentMap.get("out_diagnose_name"));
                    //离院方式
                    doc.put("discharge_kind", settlementOfPaymentMap.get("discharge_kind"));
                    //医院类别
                    doc.put("medical_type", settlementOfPaymentMap.get("medical_type"));
                    //医院等级
                    doc.put("medical_grade", settlementOfPaymentMap.get("medical_grade"));
                    //医院性质
                    doc.put("medical_nature", settlementOfPaymentMap.get("medical_nature"));
                }
                if (diagnosisMap != null) {
                    if (diagnosisMap.get("main_flag") != null) {
                        if (diagnosisMap.get("main_flag").equals("1")) {
                            //主诊疾病名称
                            doc.put("main_diag_name", diagnosisMap.get("diag_name"));
                            //主诊疾病代码
                            doc.put("main_diag_name_code", diagnosisMap.get("diag_code"));
                        } else {
                            //次诊疾病名称
                            doc.put("secondary_diag_name", diagnosisMap.get("diag_name"));
                            //次诊疾病代码
                            doc.put("secondary_diag_name_code", diagnosisMap.get("diag_code"));
                        }
                    }
                }
                //定点机构编码
                doc.put("medical_code", detailsMap.get("medical_code"));
                //定点机构名称
                doc.put("medical_name", detailsMap.get("medical_name"));
                //社会保障卡号
                doc.put("social_card", detailsMap.get("social_card"));
                //证件号码
                doc.put("card_id", detailsMap.get("card_id"));
                //姓名
                doc.put("patient_name", detailsMap.get("patient_name"));
                //险种类型
                doc.put("benefit_type", detailsMap.get("benefit_type"));
                //医疗类别
                doc.put("medical_mode", detailsMap.get("medical_mode"));
                //就诊号
                doc.put("eposide_id", detailsMap.get("eposide_id"));
                //单据号
                doc.put("bill_id", detailsMap.get("bill_id"));
                //单据明细号
                doc.put("bill_detail_id", detailsMap.get("bill_detail_id"));
                //门诊或住院号
                doc.put("hospital_id", detailsMap.get("hospital_id"));
                //费用发生时间
                doc.put("cost_time", detailsMap.get("cost_time"));
                //费用结算时间
                doc.put("clear_time", detailsMap.get("clear_time"));
                //医保目录编码
                doc.put("item_code", detailsMap.get("clear_time"));
                //医保目录名称
                doc.put("item_name", detailsMap.get("item_name"));
                //机构收费项目编码
                doc.put("item_code_hosp", detailsMap.get("item_code_hosp"));
                //机构收费项目名称
                doc.put("item_name_hosp", detailsMap.get("item_name_hosp"));
                //收费项目类别
                doc.put("charge_type", detailsMap.get("charge_type"));
                //费用类别
                doc.put("cost_type", detailsMap.get("cost_type"));
                //单价
                doc.put("unit_price", detailsMap.get("unit_price"));
                //限价
                doc.put("max_price", detailsMap.get("max_price"));
                //帖数
                doc.put("dose", detailsMap.get("dose"));
                //数量
                doc.put("num", detailsMap.get("num"));
                //金额
                doc.put("money", detailsMap.get("money"));
                //自付比例
                doc.put("pay_per_retio", detailsMap.get("pay_per_retio"));
                //医保范围费用
                doc.put("money_medical", detailsMap.get("money_medical"));
                //自理费用
                doc.put("money_self_pay", detailsMap.get("money_self_pay"));
                //自费费用
                doc.put("money_self_out", detailsMap.get("money_self_out"));
                //剂型
                doc.put("dosage_form", detailsMap.get("dosage_form"));
                //规格
                doc.put("spec", detailsMap.get("spec"));
                //药品剂型单位
                doc.put("pack_unit", detailsMap.get("pack_unit"));
                //生产企业
                doc.put("bus_produce", detailsMap.get("bus_produce"));
                //药品包装转化比
                doc.put("pack_retio", detailsMap.get("pack_retio"));
                //特殊病种标识
                doc.put("is_special", detailsMap.get("is_special"));
                //是否处方药
                doc.put("is_recipel", detailsMap.get("is_recipel"));
                //单复方标志
                doc.put("is_single", detailsMap.get("is_single"));
                //处方号
                doc.put("recipel_no", detailsMap.get("recipel_no"));
                //科室名称
                doc.put("dept_name", detailsMap.get("dept_name"));
                //执行科室名称
                doc.put("discharge_dept_name", detailsMap.get("discharge_dept_name"));
                //医生编码
                doc.put("doctor_code", detailsMap.get("doctor_code"));
                //医生姓名
                doc.put("doctor_name", detailsMap.get("doctor_name"));
                //患者住址
                doc.put("address", "");
                //病案首页号
                doc.put("medical_record_id", "");
                //个人账户支付
                doc.put("money_self_account", "");
                //个人现金支付
                doc.put("money_self_cash", "");
                //大病支付
                doc.put("money_serious", "");
                //公务员补助
                doc.put("money_subsidy_functionary", "");
                //民政基金
                doc.put("money_civil_administration", "");
                //残联基金
                doc.put("money_federation_disabled", "");
                //其他补助
                doc.put("money_subsidy_other", "");
                //家庭共济账户支付
                doc.put("money_family", "");
                //当年账户支付额
                doc.put("money_present", "");
                //历年账户支付额
                doc.put("money_previous", "");
                //个人自负
                doc.put("money_self_in", "");
                //结算前当年账户余额
                doc.put("money_this_before", "");
                //结算前历年账户余额
                doc.put("money_previous_before", "");
                //结算状态
                doc.put("clear_status", "");
                //结算方式
                doc.put("clear_kind", "");
                //医保付费方式
                doc.put("pay_way", "");
                //医疗费用支付方式
                doc.put("pay_kind", "");
                //床位号
                doc.put("bed_no", "");
                //冲销单据号
                doc.put("refund_bill_id", "");


                batchDocuments.add(doc);
                if (batchDocuments.size() >= Integer.parseInt(environment.getProperty("putIn"))) {
                    collection.insertMany(batchDocuments, options);
                    batchDocuments.clear();
                }
            }
            if (batchDocuments.size() > 0) {
                collection.insertMany(batchDocuments, options);
                batchDocuments.clear();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * 大宽表构建查询条件
     *
     * @param criteria       过滤条件
     * @param detailsLibrary 明细库
     * @param skip           跳过数
     * @param limit          分页数
     * @return 返回聚合结果
     */
    private AggregationResults<Map> wideBuildingQuery(Criteria criteria, String detailsLibrary, int skip, int limit) {
        TypedAggregation<Map> TypedAggregation = Aggregation.newAggregation(
                Map.class,
                Aggregation.limit(limit),
                Aggregation.skip(skip),
                Aggregation.match(criteria),
                Aggregation.group("bill_detail_id")
        ).withOptions(Aggregation.newAggregationOptions().allowDiskUse(true).build());

        AggregationResults<Map> aggregationResults = mongoTemplate.aggregate(
                TypedAggregation,
                detailsLibrary,
                Map.class);
        return aggregationResults;
    }
    
    /**
     * 读取csv并且入库
     *
     * @param filepath       文件位置
     * @param collectionName 入库名
     * @param target         写入目标
     * @param zoneMin        zone最小范围
     * @param zoneMax        zone最大范围
     * @param columnSize     列数量
     * @param readLineNum    从哪行开始读取
     */
    public void readAndLoad(String filepath, String collectionName, String target, int zoneMin, int zoneMax, int columnSize, long readLineNum) {
        String connectionString = "mongodb://" + this.host + ":" + this.port;
        // 构建 MongoClientSettings
        MongoClientSettings settings;
        settings = MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(connectionString))
                .writeConcern(UNACKNOWLEDGED)
                .build();

        InsertManyOptions options = new InsertManyOptions()
                .ordered(false);
        List<Document> batchDocuments = new ArrayList<>();
        //开启MongoDB
        String dbName = "ns-saas";
        try (MongoClient mongoClient = MongoClients.create(settings)) {
            MongoDatabase database = mongoClient.getDatabase(dbName);
            MongoCollection<Document> collection = database.getCollection(collectionName);


            BufferedReader reader = new BufferedReader(new FileReader(filepath));
            CSVReader csvReader = new CSVReader(reader);
            //拼接换行所需
            List<String> joint = new ArrayList<>();
            String[] s;
            //zone 跳过数
            //Long skip = (long) (theadId * amountData);
            int zone = zoneMin;
            while ((s = csvReader.readNext()) != null) {
                if (csvReader.getLinesRead() >= readLineNum) {
                    //一行的内容
                    List<String> line = new ArrayList<>(Arrays.asList(s));
                    if (line.size() < columnSize) {
                        //拼接
                        joint.addAll(line);
                        //满足一条
                        if (joint.size() == columnSize) {
                            //拼接后入库
                            Document doc = new Document();
                            gapField(joint, doc, target, zone, zoneMin, zoneMax);
                            joint.clear();
                            batchDocuments.add(doc);
                            if (batchDocuments.size() >= Integer.parseInt(environment.getProperty("putIn"))) {
                                collection.insertMany(batchDocuments, options);
                                batchDocuments.clear();
                            }
                        }
                    } else if (line.size() == columnSize) {
                        // 正常入库
                        Document doc = new Document();
                        gapField(line, doc, target, zone, zoneMin, zoneMax);
                        batchDocuments.add(doc);
                        if (batchDocuments.size() >= Integer.parseInt(environment.getProperty("putIn"))) {
                            collection.insertMany(batchDocuments, options);
                            batchDocuments.clear();
                        }
                    }
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
     * 字段统一写入
     *
     * @param line    csv单行数据
     * @param doc     写入单条数据
     * @param target  写入目标
     * @param zone    zone
     * @param zoneMin zone最小范围
     * @param zoneMax zone最大范围
     */
    private void gapField(List<String> line, Document doc, String target, int zone, int zoneMin, int zoneMax) {
        //系统字段
        doc.put("create_time", System.currentTimeMillis());
        doc.put("create_account", "admin");
        doc.put("data_status", "已归档");
        doc.put("data_type", 1);
        doc.put("priority", "");
        doc.put("bind_id", "");
        doc.put("corp_id", environment.getProperty("corp_id"));//变量
        doc.put("parent_corp_id_list", "");
        doc.put("bind_category_id", "");
        //业务字段
        switch (target) {
            case "diagnosis":
                //分类id
                doc.put("category_id", environment.getProperty("diagnosisCategoryId"));
                diagnosisField(line, doc);
                break;
            case "settlementOfPayment":
                doc.put("category_id", environment.getProperty("settlementOfPaymentCategoryId"));
                settlementOfPaymentField(line, doc);
                break;
            case "details":
                doc.put("category_id", environment.getProperty("detailsCategoryId"));
                detailsField(line, doc);
        }
        //zone
        doc.put("zone", zone++);
        if (zone >= zoneMax){
            zone = zoneMin;
        }
        // doc.put("zone", i + skip);
    }

    /**
     * 诊断字段
     *
     * @param list 数据来源
     * @param doc  写入单条数据
     */
    private void diagnosisField(List<String> list, Document doc) {
        //就诊号
        doc.put("eposide_id", list.get(Integer.parseInt(environment.getProperty("diagnosis.eposide_id"))));
        //出入院诊断类别
        doc.put("inout_diag_type", list.get(Integer.parseInt(environment.getProperty("diagnosis.inout_diag_type"))));
        //诊断类别
        doc.put("diag_type", list.get(Integer.parseInt(environment.getProperty("diagnosis.diag_type"))));
        //是否为主诊
        doc.put("main_flag", list.get(Integer.parseInt(environment.getProperty("diagnosis.main_flag"))));
        //诊断代码
        doc.put("diag_code", list.get(Integer.parseInt(environment.getProperty("diagnosis.diag_code"))));
        //诊断名称
        doc.put("diag_name", list.get(Integer.parseInt(environment.getProperty("diagnosis.diag_name"))));
        //入院病情
        doc.put("adm_cond", list.get(Integer.parseInt(environment.getProperty("diagnosis.adm_cond"))));
        //诊断科室
        doc.put("diag_dept", list.get(Integer.parseInt(environment.getProperty("diagnosis.diag_dept"))));
        //诊断医师代码
        doc.put("diag_dr_code", list.get(Integer.parseInt(environment.getProperty("diagnosis.diag_dr_code"))));
        //诊断医师姓名
        doc.put("diag_dr_name", list.get(Integer.parseInt(environment.getProperty("diagnosis.diag_dr_name"))));
        //诊断时间
        doc.put("diag_time", list.get(Integer.parseInt(environment.getProperty("diagnosis.diag_time"))));
    }

    /**
     * 结算字段
     *
     * @param list 数据来源
     * @param doc  写入单条数据
     */
    private void settlementOfPaymentField(List<String> list, Document doc) {
        //结算单据号
        doc.put("bill_id", list.get(Integer.parseInt(environment.getProperty("settlementOfPayment.bill_id"))));
        //统筹区名称
        doc.put("area_name", list.get(Integer.parseInt(environment.getProperty("settlementOfPayment.area_name"))));
        //参保人统筹区名称
        doc.put("area_name_person", list.get(Integer.parseInt(environment.getProperty("settlementOfPayment.area_name_person"))));
        //医保年度
        doc.put("year", list.get(Integer.parseInt(environment.getProperty("settlementOfPayment.year"))));
        //定点机构编码
        doc.put("medical_code", list.get(Integer.parseInt(environment.getProperty("settlementOfPayment.medical_code"))));
        //定点机构名称
        doc.put("medical_name", list.get(Integer.parseInt(environment.getProperty("settlementOfPayment.medical_name"))));
        //定点机构类别
        doc.put("medical_type", list.get(Integer.parseInt(environment.getProperty("settlementOfPayment.medical_type"))));
        //定点机构等级
        doc.put("medical_grade", list.get(Integer.parseInt(environment.getProperty("settlementOfPayment.medical_grade"))));
        //定点机构性质
        doc.put("medical_nature", list.get(Integer.parseInt(environment.getProperty("settlementOfPayment.medical_nature"))));
        //社会保障卡号
        doc.put("social_card", list.get(Integer.parseInt(environment.getProperty("settlementOfPayment.social_card"))));
        //患者证件号码
        doc.put("card_id", list.get(Integer.parseInt(environment.getProperty("settlementOfPayment.card_id"))));
        //患者姓名
        doc.put("patient_name", list.get(Integer.parseInt(environment.getProperty("settlementOfPayment.patient_name"))));
        //患者出生日期
        doc.put("birthday", list.get(Integer.parseInt(environment.getProperty("settlementOfPayment.birthday"))));
        //患者年龄
        Document age = pack("岁", list.get(Integer.parseInt(environment.getProperty("settlementOfPayment.age"))));
        doc.put("age", age);
        //性别
        doc.put("sex", list.get(Integer.parseInt(environment.getProperty("settlementOfPayment.sex"))));
        //单位名称
        doc.put("company_name", list.get(Integer.parseInt(environment.getProperty("settlementOfPayment.company_name"))));
        //险种类型
        doc.put("benefit_type", list.get(Integer.parseInt(environment.getProperty("settlementOfPayment.benefit_type"))));
        //医疗类别
        doc.put("medical_mode", list.get(Integer.parseInt(environment.getProperty("settlementOfPayment.medical_mode"))));
        //就诊号
        doc.put("eposide_id", list.get(Integer.parseInt(environment.getProperty("settlementOfPayment.eposide_id"))));
        //费用结算时间
        doc.put("clear_time", list.get(Integer.parseInt(environment.getProperty("settlementOfPayment.clear_time"))));
        //医疗费总额
        Document moneyTotal = pack("元", list.get(Integer.parseInt(environment.getProperty("settlementOfPayment.money_total"))));
        doc.put("money_total", moneyTotal);
        //医保范围费用
        Document moneyMedical = pack("元", list.get(Integer.parseInt(environment.getProperty("settlementOfPayment.money_medical"))));
        doc.put("money_medical", moneyMedical);
        //特殊病种标识
        doc.put("is_special", list.get(Integer.parseInt(environment.getProperty("settlementOfPayment.is_special"))));
        //入院日期
        doc.put("in_date", list.get(Integer.parseInt(environment.getProperty("settlementOfPayment.in_date"))));
        //出院日期
        doc.put("out_date", list.get(Integer.parseInt(environment.getProperty("settlementOfPayment.out_date"))));
        //住院天数
        Document hospitalNum = pack("天", list.get(Integer.parseInt(environment.getProperty("settlementOfPayment.hospital_num"))));
        doc.put("hospital_num", hospitalNum);
        //入院诊断疾病编码
        doc.put("in_diagnose_code", list.get(Integer.parseInt(environment.getProperty("settlementOfPayment.in_diagnose_code"))));
        //入院疾病名称
        doc.put("in_diagnose_name", list.get(Integer.parseInt(environment.getProperty("settlementOfPayment.in_diagnose_name"))));
        //出院疾病诊断编码
        doc.put("out_diagnose_code", list.get(Integer.parseInt(environment.getProperty("settlementOfPayment.out_diagnose_code"))));
        //出院疾病名称
        doc.put("out_diagnose_name", list.get(Integer.parseInt(environment.getProperty("settlementOfPayment.out_diagnose_name"))));
        //入院科室名称
        doc.put("dept_name", list.get(Integer.parseInt(environment.getProperty("settlementOfPayment.dept_name"))));
        //出院科室名称
        doc.put("out_dept_name", list.get(Integer.parseInt(environment.getProperty("settlementOfPayment.out_dept_name"))));
        //主治医生代码
        doc.put("doctor_code", list.get(Integer.parseInt(environment.getProperty("settlementOfPayment.doctor_code"))));
        //主治医生
        doc.put("doctor_name", list.get(Integer.parseInt(environment.getProperty("settlementOfPayment.doctor_name"))));
        //离院方式
        doc.put("discharge_kind", list.get(Integer.parseInt(environment.getProperty("settlementOfPayment.discharge_kind"))));
        //异地就诊标志
        doc.put("nonlocal_org_sign", list.get(Integer.parseInt(environment.getProperty("settlementOfPayment.nonlocal_org_sign"))));
    }

    /**
     * 明细字段
     *
     * @param list 数据来源
     * @param doc  写入单条数据
     */
    private void detailsField(List<String> list, Document doc) {
//        统筹区划代码
        doc.put("area_code", list.get(Integer.parseInt(environment.getProperty("details.area_code"))));
//        参保人统筹区代码
        doc.put("area_person_code", list.get(Integer.parseInt(environment.getProperty("details.area_person_code"))));
//        定点机构编码
        doc.put("medical_code", list.get(Integer.parseInt(environment.getProperty("details.medical_code"))));
//        定点机构名称
        doc.put("medical_name", list.get(Integer.parseInt(environment.getProperty("details.medical_name"))));
//        社会保障卡号
        doc.put("social_card", list.get(Integer.parseInt(environment.getProperty("details.social_card"))));
//        证件号码
        doc.put("card_id", list.get(Integer.parseInt(environment.getProperty("details.card_id"))));
//        人员编号
        doc.put("psn_no", list.get(Integer.parseInt(environment.getProperty("details.psn_no"))));
//        姓名
        doc.put("patient_name", list.get(Integer.parseInt(environment.getProperty("details.patient_name"))));
//        险种类型
        doc.put("benefit_type", list.get(Integer.parseInt(environment.getProperty("details.benefit_type"))));
//        医疗类别
        doc.put("medical_mode", list.get(Integer.parseInt(environment.getProperty("details.medical_mode"))));
//        就诊号
        doc.put("eposide_id", list.get(Integer.parseInt(environment.getProperty("details.eposide_id"))));
//        单据号
        doc.put("bill_id", list.get(Integer.parseInt(environment.getProperty("details.bill_id"))));
//        单据明细号
        doc.put("bill_detail_id", list.get(Integer.parseInt(environment.getProperty("details.bill_detail_id"))));
//         门诊或住院号
        doc.put("hospital_id", list.get(Integer.parseInt(environment.getProperty("details.hospital_id"))));
//        费用发生时间
        doc.put("cost_time", list.get(Integer.parseInt(environment.getProperty("details.cost_time"))));
//        费用结算时间
        doc.put("clear_time", list.get(Integer.parseInt(environment.getProperty("details.clear_time"))));
//        医保目录编码
        doc.put("item_code", list.get(Integer.parseInt(environment.getProperty("details.item_code"))));
//        医保目录名称
        doc.put("item_name", list.get(Integer.parseInt(environment.getProperty("details.item_name"))));
//        机构收费项目编码
        doc.put("item_code_hosp", list.get(Integer.parseInt(environment.getProperty("details.item_code_hosp"))));
//        机构收费项目名称
        doc.put("item_name_hosp", list.get(Integer.parseInt(environment.getProperty("details.item_name_hosp"))));
//        收费项目类别
        doc.put("charge_type", list.get(Integer.parseInt(environment.getProperty("details.charge_type"))));
//         费用类别
        doc.put("cost_type", list.get(Integer.parseInt(environment.getProperty("details.cost_type"))));
//        单价
        Document unitPrice = pack("元", list.get(Integer.parseInt(environment.getProperty("details.unit_price"))));
        doc.put("unit_price", unitPrice);
//        限价
        Document maxPrice = pack("元", list.get(Integer.parseInt(environment.getProperty("details.max_price"))));
        doc.put("max_price", maxPrice);
//        帖数
        Document dose = pack("个", list.get(Integer.parseInt(environment.getProperty("details.dose"))));
        doc.put("dose", dose);
//        数量
        Document num = pack("个", list.get(Integer.parseInt(environment.getProperty("details.num"))));
        doc.put("num", num);
//        金额
        Document money = pack("元", list.get(Integer.parseInt(environment.getProperty("details.money"))));
        doc.put("money", money);
//        自付比例
        Document payPerRetio = pack("比", list.get(Integer.parseInt(environment.getProperty("details.pay_per_retio"))));
        doc.put("pay_per_retio", payPerRetio);
//        医保范围费用
        Document moneyMedical = pack("元", list.get(Integer.parseInt(environment.getProperty("details.money_medical"))));
        doc.put("money_medical", moneyMedical);
//        自理费用
        Document moneySelfPay = pack("元", list.get(Integer.parseInt(environment.getProperty("details.money_self_pay"))));
        doc.put("money_self_pay", moneySelfPay);
//        自费费用
        Document moneySelfOut = pack("元", list.get(Integer.parseInt(environment.getProperty("details.money_self_out"))));
        doc.put("money_self_out", moneySelfOut);
//        剂型
        doc.put("dosage_form", list.get(Integer.parseInt(environment.getProperty("details.dosage_form"))));
//        规格
        doc.put("spec", list.get(Integer.parseInt(environment.getProperty("details.spec"))));
//        药品剂型单位
        doc.put("pack_unit", list.get(Integer.parseInt(environment.getProperty("details.pack_unit"))));
//        生产企业
        doc.put("bus_produce", list.get(Integer.parseInt(environment.getProperty("details.bus_produce"))));
//        药品包装转化比
        Document packRetio = pack("比", list.get(Integer.parseInt(environment.getProperty("details.pack_retio"))));
        doc.put("pack_retio", packRetio);
//        特殊病种标识
        doc.put("is_special", list.get(Integer.parseInt(environment.getProperty("details.is_special"))));
//        是否处方药
        doc.put("is_recipel", list.get(Integer.parseInt(environment.getProperty("details.is_recipel"))));
//        单复方标志
        doc.put("is_single", list.get(Integer.parseInt(environment.getProperty("details.is_single"))));
//        处方号
        doc.put("recipel_no", list.get(Integer.parseInt(environment.getProperty("details.recipel_no"))));
//        科室名称
        doc.put("dept_name", list.get(Integer.parseInt(environment.getProperty("details.dept_name"))));
//        执行科室名称
        doc.put("discharge_dept_name", list.get(Integer.parseInt(environment.getProperty("details.discharge_dept_name"))));
//        医生编码
        doc.put("doctor_code", list.get(Integer.parseInt(environment.getProperty("details.doctor_code"))));
//       医生姓名
        doc.put("doctor_name", list.get(Integer.parseInt(environment.getProperty("details.doctor_name"))));
    }

    /**
     * 包装量纲
     *
     * @param unit  单位
     * @param value 值
     * @return 包装后数值
     */
    private Document pack(String unit, Object value) {
        Document map = new Document();
        map.put("unit", unit);
        map.put("value", value);
        return map;
    }
}
