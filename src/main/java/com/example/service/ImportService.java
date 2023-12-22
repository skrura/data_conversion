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
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.aggregation.TypedAggregation;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;


import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.CountDownLatch;

import static com.mongodb.WriteConcern.UNACKNOWLEDGED;
import static java.lang.Math.ceil;


@Service
public class ImportService {

    @Autowired(required = false)
    private MongoTemplate mongoTemplate;

    @Value("${spring.data.mongodb.database}")
    private String dbName;
    @Value("${spring.data.mongodb.port}")
    private String port;
    @Value("${spring.data.mongodb.host}")
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
        CountDownLatch latch = new CountDownLatch(theadNum);
        long readLineNum = (long) ceil((double) rowsSize / theadNum);
        //开启线程
        for (int i = 0; i < theadNum; i++) {
            int finalI = i;
            new Thread(() -> {
                //readAndLoad(filepath, collectionName, target, zoneMin, zoneMax, columnSize, finalI * readLineNum, (finalI + 1) * readLineNum, finalI);
                testread(collectionName);
                latch.countDown();
            }).start();
        }
        try {
            latch.await();
        } catch (Exception e) {
            e.printStackTrace();
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
                if (detailsMap == null || detailsMap.isEmpty()) {
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
     * @param beginLine      从哪行开始读取
     * @param endLine        从哪行结束读取
     */
    public void readAndLoad(String filepath, String collectionName, String target, int zoneMin, int zoneMax, int columnSize, long beginLine, long endLine, int theadL) {
        long thbegin = System.currentTimeMillis();
        System.out.println("=======线程 " + theadL + "启动=======");
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
        try (MongoClient mongoClient = MongoClients.create(settings)) {
            MongoDatabase database = mongoClient.getDatabase(dbName);
            MongoCollection<Document> collection = database.getCollection(collectionName);

            BufferedReader reader = new BufferedReader(new FileReader(filepath), 40960);
            //拼接换行所需
            List<String> joint = new ArrayList<>();
            String s;
            //zone 跳过数
            //Long skip = (long) (theadId * amountData);
            Integer zone = zoneMin;
            System.out.println("=======线程" + theadL + "开始读取======");
            while ((s = reader.readLine()) != null) {
                //if (csvReader.getLinesRead() >= beginLine) {
                //一行的内容
                Document doc = new Document();
                s += ",1";
                List<String> line = Arrays.asList(s.split(","));
                if (line.size() < columnSize + 1) {
                    line = line.subList(0, line.size() - 1);
                    //拼接
                    joint.addAll(line);
                    //满足一条
                    if (joint.size() >= columnSize) {
                        //拼接后入库
                        gapField(joint, doc, target, zone, zoneMin, zoneMax);
                        zone++;
                        joint.clear();
                        batchDocuments.add(doc);
                        if (batchDocuments.size() >= putIn) {
                            collection.insertMany(batchDocuments, options);
                            batchDocuments.clear();
                        }
                    }
                } else if (line.size() >= columnSize + 1) {
                    // 正常入库
                    gapField(line, doc, target, zone, zoneMin, zoneMax);
                    zone++;
                    batchDocuments.add(doc);
                    if (batchDocuments.size() >= putIn) {
                        collection.insertMany(batchDocuments, options);
                        batchDocuments.clear();
                    }
                }
                // }
//                if (csvReader.getLinesRead() >= endLine-1) {
//                    break;
//                }
            }
            // 插入剩余的数据
            if (!batchDocuments.isEmpty()) {
                collection.insertMany(batchDocuments, options);
            }
            System.out.println("线程 " + theadL + " 运行结束");
            System.out.println("线程总运行时间：" + (System.currentTimeMillis() - thbegin) + "ms");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void testread(String collectionName) {
        List<Document> batchDocuments = new ArrayList<>();

        String connectionString = "mongodb://" + this.host + ":" + this.port;
        // 构建 MongoClientSettings
        MongoClientSettings settings;
        settings = MongoClientSettings
                .builder()
                .applyConnectionString(new ConnectionString(connectionString))
                .writeConcern(UNACKNOWLEDGED)
                .build();
        InsertManyOptions options = new InsertManyOptions()
                .ordered(false);

        //开启MongoDB
        try (MongoClient mongoClient = MongoClients.create(settings)) {
            MongoDatabase database = mongoClient.getDatabase(dbName);
            MongoCollection<Document> collection = database.getCollection(collectionName);
            for (int i = 0; i < 12000000; i++) {
                Document document = new Document();
                document.put("zone", i);
                batchDocuments.add(document);
                if (batchDocuments.size() >= putIn) {
                    collection.insertMany(batchDocuments, options);
                    batchDocuments.clear();
                }
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
        doc.put("corp_id", corp_id);//变量
        doc.put("parent_corp_id_list", "");
        doc.put("bind_category_id", "");
        //业务字段
        switch (target) {
            case "diagnosis":
                //分类id
                doc.put("category_id", diagnosisCategoryId);
                diagnosisField(line, doc);
                break;
            case "settlementOfPayment":
                //分类id
                doc.put("category_id", settlementOfPaymentCategoryId);
                settlementOfPaymentField(line, doc);
                break;
            case "details":
                //分类id
                doc.put("category_id", detailsCategoryId);
                detailsField(line, doc);
        }
        //zone
        doc.put("zone", zone++);
        if (zone >= zoneMax) {
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
        doc.put("eposide_id", list.get(dia_eposide_id));
        //出入院诊断类别
        doc.put("inout_diag_type", list.get(dia_inout_diag_type));
        //诊断类别
        doc.put("diag_type", list.get(dia_diag_type));
        //是否为主诊
        doc.put("main_flag", list.get(dia_main_flag));
        //诊断代码
        doc.put("diag_code", list.get(dia_diag_code));
        //诊断名称
        doc.put("diag_name", list.get(dia_diag_name));
        //入院病情
        doc.put("adm_cond", list.get(dia_adm_cond));
        //诊断科室
        doc.put("diag_dept", list.get(dia_diag_dept));
        //诊断医师代码
        doc.put("diag_dr_code", list.get(dia_diag_dr_code));
        //诊断医师姓名
        doc.put("diag_dr_name", list.get(dia_diag_dr_name));
        //诊断时间
        doc.put("diag_time", list.get(dia_diag_time));
    }

    /**
     * 结算字段
     *
     * @param list 数据来源
     * @param doc  写入单条数据
     */
    private void settlementOfPaymentField(List<String> list, Document doc) {
        //结算单据号
        doc.put("bill_id", list.get(set_bill_id));
        //统筹区名称
        doc.put("area_name", list.get(set_area_name));
        //参保人统筹区名称
        doc.put("area_name_person", list.get(set_area_name_person));
        //定点机构编码
        doc.put("medical_code", list.get(set_medical_code ));
        //定点机构名称
        doc.put("medical_name", list.get(set_medical_name));
        //定点机构类别
        doc.put("medical_type", list.get(set_medical_type));
        //定点机构等级
        doc.put("medical_grade", list.get(set_medical_grade));
        //定点机构性质
        doc.put("medical_nature", list.get(set_medical_nature));
        //社会保障卡号
        doc.put("social_card", list.get(set_social_card));
        //患者证件号码
        doc.put("card_id", list.get(set_card_id));
        //患者姓名
        doc.put("patient_name", list.get(set_patient_name));
        //患者出生日期
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        try {
            doc.put("birthday", format.parse(list.get(set_birthday)).getTime());
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
        //患者年龄
        doc.put("age", pack("岁", list.get(set_age)));
        //性别
        doc.put("sex", list.get(set_sex));
        //单位名称
        doc.put("company_name", list.get(set_company_name));
        //险种类型
        doc.put("benefit_type", list.get(set_benefit_type));
        //医疗类别
        doc.put("medical_mode", list.get(set_medical_mode));
        //就诊号
        doc.put("eposide_id", list.get(set_eposide_id));
        //费用结算时间
        SimpleDateFormat format2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try {
            doc.put("clear_time", format2.parse(list.get(set_clear_time)));
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
        //医疗费总额
        doc.put("money_total", pack("元", list.get(set_money_total)));
        //医保范围费用
        doc.put("money_medical", pack("元", list.get(set_money_medical)));
        //特殊病种标识
        doc.put("is_special", list.get(set_is_special));
        //入院日期
        try {
            doc.put("in_date", format.parse(list.get(set_in_date)).getTime());
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
        //出院日期
        try {
            doc.put("out_date", format.parse(list.get(set_out_date)).getTime());
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
        //住院天数
        doc.put("hospital_num", pack("天", list.get(set_hospital_num)));
        //入院诊断疾病编码
        doc.put("in_diagnose_code", list.get(set_in_diagnose_code));
        //入院疾病名称
        doc.put("in_diagnose_name", list.get(set_in_diagnose_name));
        //出院疾病诊断编码
        doc.put("out_diagnose_code", list.get(set_out_diagnose_code));
        //出院疾病名称
        doc.put("out_diagnose_name", list.get(set_out_diagnose_name));
        //入院科室名称
        doc.put("dept_name", list.get(set_dept_name));
        //出院科室名称
        doc.put("out_dept_name", list.get(set_out_dept_name));
        //主治医生代码
        doc.put("doctor_code", list.get(set_doctor_code));
        //主治医生
        doc.put("doctor_name", list.get(set_doctor_name));
        //离院方式
        doc.put("discharge_kind", list.get(set_discharge_kind));
        //异地就诊标志
        doc.put("nonlocal_org_sign", list.get(set_nonlocal_org_sign));
    }

    /**
     * 明细字段
     *
     * @param list 数据来源
     * @param doc  写入单条数据
     */
    private void detailsField(List<String> list, Document doc) {
//        统筹区划代码
        doc.put("area_code", list.get(det_area_code));
//        参保人统筹区代码
        doc.put("area_person_code", list.get(det_area_person_code));
//        定点机构编码
        doc.put("medical_code", list.get(det_medical_code));
//        定点机构名称
        doc.put("medical_name", list.get(det_medical_name));
//        社会保障卡号
        doc.put("social_card", list.get(det_social_card ));
//        证件号码
        doc.put("card_id", list.get(det_card_id));
//        人员编号
        doc.put("psn_no", list.get(det_psn_no));
//        姓名
        doc.put("patient_name", list.get(det_patient_name));
//        险种类型
        doc.put("benefit_type", list.get(det_benefit_type));
//        医疗类别
        doc.put("medical_mode", list.get(det_medical_mode));
//        就诊号
        doc.put("eposide_id", list.get(det_eposide_id));
//        单据号
        doc.put("bill_id", list.get(det_bill_id));
//        单据明细号
        doc.put("bill_detail_id", list.get(det_bill_detail_id));
//         门诊或住院号
        doc.put("hospital_id", list.get(det_hospital_id));
//        费用发生时间
        doc.put("cost_time", list.get(det_cost_time));
//        费用结算时间
        doc.put("clear_time", list.get(det_clear_time));
//        医保目录编码
        doc.put("item_code", list.get(det_item_code));
//        医保目录名称
        doc.put("item_name", list.get(det_item_name));
//        机构收费项目编码
        doc.put("item_code_hosp", list.get(det_item_code_hosp));
//        机构收费项目名称
        doc.put("item_name_hosp", list.get(det_item_name_hosp));
//        收费项目类别
        doc.put("charge_type", list.get(det_charge_type));
//         费用类别
        doc.put("cost_type", list.get(det_cost_type));
//        单价
        doc.put("unit_price", pack("元", list.get(det_unit_price)));
//        限价
        doc.put("max_price", pack("元", list.get(det_max_price)));
//        帖数
        doc.put("dose", pack("个", list.get(det_dose)));
//        数量
        doc.put("num", pack("个", list.get(det_num)));
//        金额
        doc.put("money", pack("元", list.get(det_money)));
//        自付比例
        doc.put("pay_per_retio", pack("比", list.get(det_pay_per_retio)));
//        医保范围费用
        doc.put("money_medical", pack("元", list.get(det_money_medical)));
//        自理费用
        doc.put("money_self_pay", pack("元", list.get(det_money_self_pay)));
//        自费费用
        doc.put("money_self_out", pack("元", list.get(det_money_self_out)));
//        剂型
        doc.put("dosage_form", list.get(det_dosage_form));
//        规格
        doc.put("spec", list.get(det_spec));
//        药品剂型单位
        doc.put("pack_unit", list.get(det_pack_unit));
//        生产企业
        doc.put("bus_produce", list.get(det_bus_produce));
//        药品包装转化比
        doc.put("pack_retio", pack("比", list.get(det_pack_retio)));
//        特殊病种标识
        doc.put("is_special", list.get(det_is_special));
//        是否处方药
        doc.put("is_recipel", list.get(det_is_recipel));
//        单复方标志
        doc.put("is_single", list.get(det_is_single));
//        处方号
        doc.put("recipel_no", list.get(det_recipel_no));
//        科室名称
        doc.put("dept_name", list.get(det_dept_name));
//        执行科室名称
        doc.put("discharge_dept_name", list.get(det_discharge_dept_name));
//        医生编码
        doc.put("doctor_code", list.get(det_doctor_code));
//       医生姓名
        doc.put("doctor_name", list.get(det_doctor_name));
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

    // 明细分类id
    @Value("detailsCategoryId")
    String detailsCategoryId = "";
    // 诊断分类id
    @Value("${diagnosisCategoryId}")
    String diagnosisCategoryId = "";
    // 结算分类id
    @Value("${settlementOfPaymentCategoryId}")
    String settlementOfPaymentCategoryId = "";
    // 入库数量
    @Value("${putIn}")
    int putIn = 0;
    // 租户id
    @Value("${corp_id}")
    String corp_id = "";


    //诊断
    //就诊号
    @Value("${diagnosis.eposide_id}")
    int dia_eposide_id = 1;
    //出入院诊断类别
    @Value("${diagnosis.inout_diag_type}")
    int dia_inout_diag_type = 2;
    //诊断类别
    @Value("${diagnosis.diag_type}")
    int dia_diag_type = 3;
    //是否为主诊
    @Value("${diagnosis.main_flag}")
    int dia_main_flag = 4;
    //诊断代码
    @Value("${diagnosis.diag_code}")
    int dia_diag_code = 5;
    //诊断名称
    @Value("${diagnosis.diag_name}")
    int dia_diag_name = 6;
    //入院病情
    @Value("${diagnosis.adm_cond}")
    int dia_adm_cond = 7;
    //诊断科室
    @Value("${diagnosis.diag_dept}")
    int dia_diag_dept = 8;
    //诊断医师代码
    @Value("${diagnosis.diag_dr_code}")
    int dia_diag_dr_code = 9;
    //诊断医师姓名
    @Value("${diagnosis.diag_dr_name}")
    int dia_diag_dr_name = 10;
    //诊断时间
    @Value("${diagnosis.diag_time}")
    int dia_diag_time = 11;


    //结算
    //结算单据号
    @Value("${settlementOfPayment.bill_id}")
    int set_bill_id = 0;
    //统筹区名称
    @Value("${settlementOfPayment.area_name}")
    int set_area_name = 0;
    //参保人统筹区名称
    @Value("${settlementOfPayment.area_name_person}")
    int set_area_name_person = 0;
    //定点机构编码
    @Value("${settlementOfPayment.medical_code}")
    int set_medical_code = 0;
    //定点机构名称
    @Value("${settlementOfPayment.medical_name}")
    int set_medical_name = 0;
    //定点机构类别
    @Value("${settlementOfPayment.medical_type}")
    int set_medical_type = 0;
    //定点机构等级
    @Value("${settlementOfPayment.medical_grade}")
    int set_medical_grade = 0;
    //定点机构性质
    @Value("${settlementOfPayment.medical_nature}")
    int set_medical_nature = 0;
    //社会保障卡号
    @Value("${settlementOfPayment.social_card}")
    int set_social_card = 0;
    //患者证件号码
    @Value("${settlementOfPayment.card_id}")
    int set_card_id = 0;
    //患者姓名
    @Value("${settlementOfPayment.patient_name}")
    int set_patient_name = 0;
    //患者出生日期 (转时间戳)
    @Value("${settlementOfPayment.birthday}")
    int set_birthday = 0;
    //患者年龄
    @Value("${settlementOfPayment.age}")
    int set_age = 0;
    //性别
    @Value("${settlementOfPayment.sex}")
    int set_sex = 0;
    //单位名称
    @Value("${settlementOfPayment.company_name}")
    int set_company_name = 0;
    //险种类型
    @Value("${settlementOfPayment.benefit_type}")
    int set_benefit_type = 0;
    //医疗类别
    @Value("${settlementOfPayment.medical_mode}")
    int set_medical_mode = 0;
    //就诊号
    @Value("${settlementOfPayment.eposide_id}")
    int set_eposide_id = 0;
    //费用结算时间 （时间戳）
    @Value("${settlementOfPayment.clear_time}")
    int set_clear_time = 0;
    //医疗费总额
    @Value("${settlementOfPayment.money_total}")
    int set_money_total = 0;
    //医保范围费用
    @Value("${settlementOfPayment.money_medical}")
    int set_money_medical = 0;
    //特殊病种标识
    @Value("${settlementOfPayment.is_special}")
    int set_is_special = 0;
    //入院日期
    @Value("${settlementOfPayment.in_date}")
    int set_in_date = 0;
    //出院日期
    @Value("${settlementOfPayment.out_date}")
    int set_out_date = 0;
    //住院天数
    @Value("${settlementOfPayment.hospital_num}")
    int set_hospital_num = 1;
    //入院诊断疾病编码
    @Value("${settlementOfPayment.in_diagnose_code}")
    int set_in_diagnose_code = 0;
    //入院疾病名称
    @Value("${settlementOfPayment.in_diagnose_name}")
    int set_in_diagnose_name = 0;
    //出院疾病诊断编码
    @Value("${settlementOfPayment.out_diagnose_code}")
    int set_out_diagnose_code = 0;
    //出院疾病名称
    @Value("${settlementOfPayment.out_diagnose_name}")
    int set_out_diagnose_name = 0;
    //入院科室名称
    @Value("${settlementOfPayment.dept_name}")
    int set_dept_name = 0;
    //出院科室名称
    @Value("${settlementOfPayment.out_dept_name}")
    int set_out_dept_name = 0;
    //主治医生代码
    @Value("${settlementOfPayment.doctor_code}")
    int set_doctor_code = 0;
    //主治医生
    @Value("${settlementOfPayment.doctor_name}")
    int set_doctor_name = 0;
    //离院方式
    @Value("${settlementOfPayment.discharge_kind}")
    int set_discharge_kind = 0;
    //异地就诊标志
    @Value("${settlementOfPayment.nonlocal_org_sign}")
    int set_nonlocal_org_sign = 0;

    //统筹区划代码
    @Value("${details.area_code}")
    int det_area_code = 1;
    //参保人统筹区代码
    @Value("${details.area_person_code}")
    int det_area_person_code = 2;
    //定点机构编码
    @Value("${details.medical_code}")
    int det_medical_code = 3;
    //定点机构名称
    @Value("${details.medical_name}")
    int det_medical_name = 4;
    //社会保障卡号
    @Value("${details.social_card}")
    int det_social_card = 5;
    //证件号码
    @Value("${details.card_id}")
    int det_card_id = 6;
    //人员编号
    @Value("${details.psn_no}")
    int det_psn_no = 7;
    //姓名
    @Value("${details.patient_name}")
    int det_patient_name = 8;
    //险种类型
    @Value("${details.benefit_type}")
    int det_benefit_type = 9;
    //医疗类别
    @Value("${details.medical_mode}")
    int det_medical_mode = 10;
    //就诊号
    @Value("${details.eposide_id}")
    int det_eposide_id = 11;
    //单据号
    @Value("${details.bill_id}")
    int det_bill_id = 12;
    //单据明细号
    @Value("${details.bill_detail_id}")
    int det_bill_detail_id = 13;
    //门诊或住院号
    @Value("${details.hospital_id}")
    int det_hospital_id = 14;
    //费用发生时间
    @Value("${details.cost_time}")
    int det_cost_time = 15;
    //费用结算时间
    @Value("${details.clear_time}")
    int det_clear_time = 16;
    //医保目录编码
    @Value("${details.item_code}")
    int det_item_code = 17;
    //医保目录名称
    @Value("${details.item_name}")
    int det_item_name = 18;
    //机构收费项目编码
    @Value("${details.item_code_hosp}")
    int det_item_code_hosp = 19;
    //机构收费项目名称
    @Value("${details.item_name_hosp}")
    int det_item_name_hosp = 20;
    //收费项目类别
    @Value("${details.charge_type}")
    int det_charge_type = 21;
    //费用类别
    @Value("${details.cost_type}")
    int det_cost_type = 22;
    //单价
    @Value("${details.unit_price}")
    int det_unit_price = 23;
    //限价
    @Value("${details.max_price}")
    int det_max_price = 24;
    //帖数
    @Value("${details.dose}")
    int det_dose = 25;
    //数量
    @Value("${details.num}")
    int det_num = 26;
    //金额
    @Value("${details.money}")
    int det_money = 27;
    //自付比例
    @Value("${details.pay_per_retio}")
    int det_pay_per_retio = 28;
    //医保范围费用
    @Value("${details.money_medical}")
    int det_money_medical = 29;
    //自理费用
    @Value("${details.money_self_pay}")
    int det_money_self_pay = 30;
    //自费费用
    @Value("${details.money_self_out}")
    int det_money_self_out = 31;
    //剂型
    @Value("${details.dosage_form}")
    int det_dosage_form = 32;
    //规格
    @Value("${details.spec}")
    int det_spec = 33;
    //药品剂型单位
    @Value("${details.pack_unit}")
    int det_pack_unit = 34;
    //生产企业
    @Value("${details.bus_produce}")
    int det_bus_produce = 35;
    //药品包装转化比
    @Value("${details.pack_retio}")
    int det_pack_retio = 36;
    //特殊病种标识
    @Value("${details.is_special}")
    int det_is_special = 37;
    //是否处方药
    @Value("${details.is_recipel}")
    int det_is_recipel = 38;
    //单复方标志
    @Value("${details.is_single}")
    int det_is_single = 39;
    //处方号
    @Value("${details.recipel_no}")
    int det_recipel_no = 40;
    //科室名称
    @Value("${details.dept_name}")
    int det_dept_name = 41;
    //执行科室名称
    @Value("${details.discharge_dept_name}")
    int det_discharge_dept_name = 42;
    //医生编码
    @Value("${details.doctor_code}")
    int det_doctor_code = 43;
    //医生姓名
    @Value("${details.doctor_name}")
    int det_doctor_name = 44;

}
