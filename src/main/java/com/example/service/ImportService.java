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
import com.opencsv.exceptions.CsvValidationException;
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

    @Autowired
    private Environment environment;
    private static int theadData = 500000;

    /**
     * 原始三张表
     *
     * @param collectionName
     * @param filepath
     * @param target
     * @param zoneMin
     * @param zoneMax
     * @param size
     * @param theadNum
     * @return
     */
    public String importsDataOpt(String collectionName, String filepath, String target, int zoneMin, int zoneMax, int size, int theadNum) {
        filepath = "D:\\桌面\\第一人民门诊明细.csv";
        //转换数据方便数据分发
        Collection<List<String>> csvData = dataExtraction(filepath, size, theadNum);
        //数据转换
        List<List<String>> result = new ArrayList<>(csvData);
        //数据分发
        List<List<List<String>>> csvDataList = dataDistribution(result, theadNum);
        CountDownLatch latch = new CountDownLatch(theadNum);
        //开启线程
        for (int i = 0; i < theadNum; i++) {
            int finalI = i;
            new Thread(() -> {
                //主要导库操作
                read(csvDataList.get(finalI), collectionName, target, zoneMin, zoneMax, finalI);
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
     * @param collectionName
     * @param theadNum
     * @param dataSize
     * @return
     */
    public String wideControl(String collectionName, int theadNum, int dataSize) {
        int theadDataNum = (int) (Math.ceil((double) dataSize / theadNum));
        CountDownLatch latch = new CountDownLatch(theadNum);
        // 固定线程数线程池
        ExecutorService executor = Executors.newFixedThreadPool(theadNum);
        for (int i = 0; i < theadNum; i++) {
            int finalI = i;
            executor.execute(() -> {
                wideImport(finalI * theadDataNum, theadDataNum, collectionName);
                latch.countDown();
            });
        }
        while (!executor.isTerminated()) {
            // 等待所有任务完成
            latch.countDown();
        }
        return "Wide Complete";
    }


    /**
     * 导入大宽表
     *
     * @param skip
     * @param theadDataNum
     * @param collectionName
     */
    private void wideImport(int skip, int theadDataNum, String collectionName) {
        String mingxi = environment.getProperty("mingxiku");
        String zhenduan = environment.getProperty("zhenduanku");
        String jiesuan = environment.getProperty("jiesuanku");
        // 初始条件（大前提）
        Criteria criteria = Criteria.where("benefit_type").in("本地职工", "本地居保", "省内异地", "省外异地");
        //根据单据明细号分组
        List<Map> mapList = wideBuildingQuery(criteria, mingxi, zhenduan, jiesuan, skip, theadDataNum).getMappedResults();


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
            List<Document> batchDocuments = new ArrayList<>();

            for (Map map : mapList) {
                // 构建本地查询
                MongoTemplate mongoTemplatew = new MongoTemplate(MongoClients.create(settings), dbName);
                // 构建单据明细号查询条件
                Criteria criteria1 = Criteria.where("bill_detail_id").is(map.get("_id"));
                // 合并初始条件
                // 根据单据明细号查询明细表
                Map mingxiMap = mongoTemplatew.findOne(Query.query(new Criteria().andOperator(criteria, criteria1)), Map.class, mingxi);
                // 构建单据号查询条件
                Criteria criteria2 = Criteria.where("bill_id").is(mingxiMap.get("bill_id"));
                // 合并初始条件
                // 根据单据号查询结算表
                Map jiesuanMap = mongoTemplatew.findOne(Query.query(new Criteria().andOperator(criteria, criteria2)), Map.class, jiesuan);
                // 构建就诊号查询条件
                Criteria criteria3 = Criteria.where("eposide_id").is(mingxiMap.get("eposide_id"));
                // 合并初始条件
                // 根据就诊号查询诊断表
                Map zhenduanMap = mongoTemplatew.findOne(Query.query(new Criteria().andOperator(criteria, criteria3)), Map.class, zhenduan);
                // 单条数据
                Document doc = new Document();
                // 主要明细为空
                if (mingxiMap.isEmpty()) {
                    continue;
                }
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
                if (jiesuanMap != null) {
                    // 统筹区名称
                    doc.put("area_name", jiesuanMap.get("area_name"));
                    //参保人统筹区名称
                    doc.put("area_name_person", jiesuanMap.get("area_name_person"));
                    //医保年度
                    doc.put("year", jiesuanMap.get("year"));
                    //出生日期
                    doc.put("birthday", jiesuanMap.get("birthday"));
                    //年龄
                    doc.put("age", jiesuanMap.get("age"));
                    //性别
                    doc.put("sex", jiesuanMap.get("sex"));
                    //单位名称
                    doc.put("company_name", jiesuanMap.get("company_name"));
                    //医疗费总额
                    doc.put("money_total", jiesuanMap.get("money_total"));
                    //统筹支付金额
                    doc.put("money_bmi", jiesuanMap.get("money_medical"));
                    //入院日期
                    doc.put("in_date", jiesuanMap.get("in_date"));
                    //出院日期
                    doc.put("out_date", jiesuanMap.get("out_date"));
                    //住院天数
                    doc.put("hospital_num", jiesuanMap.get("hospital_num"));
                    //入院诊断疾病编码
                    doc.put("in_diagnose_code", jiesuanMap.get("in_diagnose_code"));
                    //入院疾病名称
                    doc.put("in_diagnose_name", jiesuanMap.get("in_diagnose_name"));
                    //出院疾病诊断编码
                    doc.put("out_diagnose_code", jiesuanMap.get("out_diagnose_code"));
                    //出院疾病名称
                    doc.put("out_diagnose_name", jiesuanMap.get("out_diagnose_name"));
                    //离院方式
                    doc.put("discharge_kind", jiesuanMap.get("discharge_kind"));
                    //医院类别
                    doc.put("medical_type", jiesuanMap.get("medical_type"));
                    //医院等级
                    doc.put("medical_grade", jiesuanMap.get("medical_grade"));
                    //医院性质
                    doc.put("medical_nature", jiesuanMap.get("medical_nature"));
                }
                if (zhenduanMap != null) {
                    if (zhenduanMap.get("main_flag") != null) {
                        if (zhenduanMap.get("main_flag").equals("1")) {
                            //主诊疾病名称
                            doc.put("main_diag_name", zhenduanMap.get("diag_name"));
                            //主诊疾病代码
                            doc.put("main_diag_name_code", zhenduanMap.get("diag_code"));
                        } else {
                            //次诊疾病名称
                            doc.put("secondary_diag_name", zhenduanMap.get("diag_name"));
                            //次诊疾病代码
                            doc.put("secondary_diag_name_code", zhenduanMap.get("diag_code"));
                        }
                    }
                }
                //定点机构编码
                doc.put("medical_code", mingxiMap.get("medical_code"));
                //定点机构名称
                doc.put("medical_name", mingxiMap.get("medical_name"));
                //社会保障卡号
                doc.put("social_card", mingxiMap.get("social_card"));
                //证件号码
                doc.put("card_id", mingxiMap.get("card_id"));
                //姓名
                doc.put("patient_name", mingxiMap.get("patient_name"));
                //险种类型
                doc.put("benefit_type", mingxiMap.get("benefit_type"));
                //医疗类别
                doc.put("medical_mode", mingxiMap.get("medical_mode"));
                //就诊号
                doc.put("eposide_id", mingxiMap.get("eposide_id"));
                //单据号
                doc.put("bill_id", mingxiMap.get("bill_id"));
                //单据明细号
                doc.put("bill_detail_id", mingxiMap.get("bill_detail_id"));
                //门诊或住院号
                doc.put("hospital_id", mingxiMap.get("hospital_id"));
                //费用发生时间
                doc.put("cost_time", mingxiMap.get("cost_time"));
                //费用结算时间
                doc.put("clear_time", mingxiMap.get("clear_time"));
                //医保目录编码
                doc.put("item_code", mingxiMap.get("clear_time"));
                //医保目录名称
                doc.put("item_name", mingxiMap.get("item_name"));
                //机构收费项目编码
                doc.put("item_code_hosp", mingxiMap.get("item_code_hosp"));
                //机构收费项目名称
                doc.put("item_name_hosp", mingxiMap.get("item_name_hosp"));
                //收费项目类别
                doc.put("charge_type", mingxiMap.get("charge_type"));
                //费用类别
                doc.put("cost_type", mingxiMap.get("cost_type"));
                //单价
                doc.put("unit_price", mingxiMap.get("unit_price"));
                //限价
                doc.put("max_price", mingxiMap.get("max_price"));
                //帖数
                doc.put("dose", mingxiMap.get("dose"));
                //数量
                doc.put("num", mingxiMap.get("num"));
                //金额
                doc.put("money", mingxiMap.get("money"));
                //自付比例
                doc.put("pay_per_retio", mingxiMap.get("pay_per_retio"));
                //医保范围费用
                doc.put("money_medical", mingxiMap.get("money_medical"));
                //自理费用
                doc.put("money_self_pay", mingxiMap.get("money_self_pay"));
                //自费费用
                doc.put("money_self_out", mingxiMap.get("money_self_out"));
                //剂型
                doc.put("dosage_form", mingxiMap.get("dosage_form"));
                //规格
                doc.put("spec", mingxiMap.get("spec"));
                //药品剂型单位
                doc.put("pack_unit", mingxiMap.get("pack_unit"));
                //生产企业
                doc.put("bus_produce", mingxiMap.get("bus_produce"));
                //药品包装转化比
                doc.put("pack_retio", mingxiMap.get("pack_retio"));
                //特殊病种标识
                doc.put("is_special", mingxiMap.get("is_special"));
                //是否处方药
                doc.put("is_recipel", mingxiMap.get("is_recipel"));
                //单复方标志
                doc.put("is_single", mingxiMap.get("is_single"));
                //处方号
                doc.put("recipel_no", mingxiMap.get("recipel_no"));
                //科室名称
                doc.put("dept_name", mingxiMap.get("dept_name"));
                //执行科室名称
                doc.put("discharge_dept_name", mingxiMap.get("discharge_dept_name"));
                //医生编码
                doc.put("doctor_code", mingxiMap.get("doctor_code"));
                //医生姓名
                doc.put("doctor_name", mingxiMap.get("doctor_name"));
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
     * @param criteria
     * @param mingxi
     * @param zhenduan
     * @param jiesuan
     * @param skip
     * @param limit
     * @return
     */

    private AggregationResults<Map> wideBuildingQuery(Criteria criteria, String mingxi, String zhenduan, String jiesuan, int skip, int limit) {
        TypedAggregation<Map> TypedAggregation = Aggregation.newAggregation(
                Map.class,
                Aggregation.limit(limit),
                Aggregation.skip(skip),
                Aggregation.match(criteria),
                Aggregation.group("bill_detail_id")
        ).withOptions(Aggregation.newAggregationOptions().allowDiskUse(true).build());

        AggregationResults<Map> aggregationResults = mongoTemplate.aggregate(
                TypedAggregation,
                mingxi,
                Map.class);
        return aggregationResults;
    }


    /**
     * @param filepath
     * @param size
     * @return
     */
    private Collection<List<String>> dataExtraction(String filepath, int size, int theadNum) {
        try {
            // 设置并行流线程数
            System.setProperty("java.util.concurrent.ForkJoinPool.common.parallelism", String.valueOf(theadNum));
            //初始化线程安全集合存储数据 一条行数据对应一个List<String>
            Collection<List<String>> list = Collections.synchronizedCollection(new ArrayList<>());
            // 获取文件
            File touch = FileUtil.touch(filepath);
            // 创建一个文件输入流
            FileInputStream fileInputStream = IoUtil.toStream(touch);
            // 创建一个可以读取UTF-8编码的BufferedReader
            BufferedReader utf8Reader = IoUtil.getUtf8Reader(fileInputStream);
            // 读取所有行并转换为Stream(延迟求值) 需要处理才会读取
            Stream<String> lines = utf8Reader.lines();
            //System.out.println(lines.count());
            // 对每一行进行并行处理
            lines.parallel().forEach(s -> {
                List<String> next = new ArrayList<>();
                // 拼接换行数据需要
                Collection<String> joint = Collections.synchronizedCollection(new ArrayList<>());
                try (CSVReader reader = new CSVReader(new StringReader(s))) {
                    // 读取下一行并添加到列表中
                    next = Arrays.asList(reader.readNext());
                    if (next.size() < size) {
                        synchronized (this) {
                            joint.addAll(next);
                            if (joint.size() == size) {
                                List<String> jointCopy = new ArrayList<>(joint); // 创建joint的副本
                                System.out.println("换行" + jointCopy);
                                joint.clear();
                                list.add(jointCopy);
                            }
                        }
                    } else if (next.size() == size) {
                        list.add(next);
                    }
                } catch (IOException | CsvValidationException e) {
                }
            });
            return list;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 数据分发
     *
     * @param csvData
     * @return
     */
    public List<List<List<String>>> dataDistribution(List<List<String>> csvData, int theadNum) {
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
            if (i == theadNum - 1)
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
    public void read(List<List<String>> csvData, String collectionName, String target, int zoneMin, int zoneMax, int theadId) {

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
            //zone 跳过数
            //Long skip = (long) (theadId * amountData);
            int zone = zoneMin;
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
                doc.put("zone", zone++);
                if (zone >= zoneMax)
                    zone = zoneMin;
                // doc.put("zone", i + skip);
                i++;
                batchDocuments.add(doc);
                if (batchDocuments.size() >= Integer.parseInt(environment.getProperty("putIn"))) {
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
        if (list.size() < 11) {
            System.out.println("xx");
        }
        //就诊号
        doc.put("eposide_id", list.get(Integer.parseInt(environment.getProperty("zhenduan.eposide_id"))));
        //出入院诊断类别
        doc.put("inout_diag_type", list.get(Integer.parseInt(environment.getProperty("zhenduan.inout_diag_type"))));
        //诊断类别
        doc.put("diag_type", list.get(Integer.parseInt(environment.getProperty("zhenduan.diag_type"))));
        //是否为主诊
        doc.put("main_flag", list.get(Integer.parseInt(environment.getProperty("zhenduan.main_flag"))));
        //诊断代码
        doc.put("diag_code", list.get(Integer.parseInt(environment.getProperty("zhenduan.diag_code"))));
        //诊断名称
        doc.put("diag_name", list.get(Integer.parseInt(environment.getProperty("zhenduan.diag_name"))));
        //入院病情
        doc.put("adm_cond", list.get(Integer.parseInt(environment.getProperty("zhenduan.adm_cond"))));
        //诊断科室
        doc.put("diag_dept", list.get(Integer.parseInt(environment.getProperty("zhenduan.diag_dept"))));
        //诊断医师代码
        doc.put("diag_dr_code", list.get(Integer.parseInt(environment.getProperty("zhenduan.diag_dr_code"))));
        //诊断医师姓名
        doc.put("diag_dr_name", list.get(Integer.parseInt(environment.getProperty("zhenduan.diag_dr_name"))));
        //诊断时间
        doc.put("diag_time", list.get(Integer.parseInt(environment.getProperty("zhenduan.diag_time"))));
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
        doc.put("bill_id", list.get(Integer.parseInt(environment.getProperty("jiesuan.bill_id"))));
        //统筹区名称
        doc.put("area_name", list.get(Integer.parseInt(environment.getProperty("jiesuan.area_name"))));
        //参保人统筹区名称
        doc.put("area_name_person", list.get(Integer.parseInt(environment.getProperty("jiesuan.area_name_person"))));
        //医保年度
        doc.put("year", list.get(Integer.parseInt(environment.getProperty("jiesuan.year"))));
        //定点机构编码
        doc.put("medical_code", list.get(Integer.parseInt(environment.getProperty("jiesuan.medical_code"))));
        //定点机构名称
        doc.put("medical_name", list.get(Integer.parseInt(environment.getProperty("jiesuan.medical_name"))));
        //定点机构类别
        doc.put("medical_type", list.get(Integer.parseInt(environment.getProperty("jiesuan.medical_type"))));
        //定点机构等级
        doc.put("medical_grade", list.get(Integer.parseInt(environment.getProperty("jiesuan.medical_grade"))));
        //定点机构性质
        doc.put("medical_nature", list.get(Integer.parseInt(environment.getProperty("jiesuan.medical_nature"))));
        //社会保障卡号
        doc.put("social_card", list.get(Integer.parseInt(environment.getProperty("jiesuan.social_card"))));
        //患者证件号码
        doc.put("card_id", list.get(Integer.parseInt(environment.getProperty("jiesuan.card_id"))));
        //患者姓名
        doc.put("patient_name", list.get(Integer.parseInt(environment.getProperty("jiesuan.patient_name"))));
        //患者出生日期
        doc.put("birthday", list.get(Integer.parseInt(environment.getProperty("jiesuan.birthday"))));
        //患者年龄
        Document age = pack("岁", list.get(Integer.parseInt(environment.getProperty("jiesuan.age"))));
        doc.put("age", age);
        //性别
        doc.put("sex", list.get(Integer.parseInt(environment.getProperty("jiesuan.sex"))));
        //单位名称
        doc.put("company_name", list.get(Integer.parseInt(environment.getProperty("jiesuan.company_name"))));
        //险种类型
        doc.put("benefit_type", list.get(Integer.parseInt(environment.getProperty("jiesuan.benefit_type"))));
        //医疗类别
        doc.put("medical_mode", list.get(Integer.parseInt(environment.getProperty("jiesuan.medical_mode"))));
        //就诊号
        doc.put("eposide_id", list.get(Integer.parseInt(environment.getProperty("jiesuan.eposide_id"))));
        //费用结算时间
        doc.put("clear_time", list.get(Integer.parseInt(environment.getProperty("jiesuan.clear_time"))));
        //医疗费总额
        Document moneyTotal = pack("元", list.get(Integer.parseInt(environment.getProperty("jiesuan.money_total"))));
        doc.put("money_total", moneyTotal);
        //医保范围费用
        Document moneyMedical = pack("元", list.get(Integer.parseInt(environment.getProperty("jiesuan.money_medical"))));
        doc.put("money_medical", moneyMedical);
        //特殊病种标识
        doc.put("is_special", list.get(Integer.parseInt(environment.getProperty("jiesuan.is_special"))));
        //入院日期
        doc.put("in_date", list.get(Integer.parseInt(environment.getProperty("jiesuan.in_date"))));
        //出院日期
        doc.put("out_date", list.get(Integer.parseInt(environment.getProperty("jiesuan.out_date"))));
        //住院天数
        Document hospitalNum = pack("天", list.get(Integer.parseInt(environment.getProperty("jiesuan.hospital_num"))));
        doc.put("hospital_num", hospitalNum);
        //入院诊断疾病编码
        doc.put("in_diagnose_code", list.get(Integer.parseInt(environment.getProperty("jiesuan.in_diagnose_code"))));
        //入院疾病名称
        doc.put("in_diagnose_name", list.get(Integer.parseInt(environment.getProperty("jiesuan.in_diagnose_name"))));
        //出院疾病诊断编码
        doc.put("out_diagnose_code", list.get(Integer.parseInt(environment.getProperty("jiesuan.out_diagnose_code"))));
        //出院疾病名称
        doc.put("out_diagnose_name", list.get(Integer.parseInt(environment.getProperty("jiesuan.out_diagnose_name"))));
        //入院科室名称
        doc.put("dept_name", list.get(Integer.parseInt(environment.getProperty("jiesuan.dept_name"))));
        //出院科室名称
        doc.put("out_dept_name", list.get(Integer.parseInt(environment.getProperty("jiesuan.out_dept_name"))));
        //主治医生代码
        doc.put("doctor_code", list.get(Integer.parseInt(environment.getProperty("jiesuan.doctor_code"))));
        //主治医生
        doc.put("doctor_name", list.get(Integer.parseInt(environment.getProperty("jiesuan.doctor_name"))));
        //离院方式
        doc.put("discharge_kind", list.get(Integer.parseInt(environment.getProperty("jiesuan.discharge_kind"))));
        //异地就诊标志
        doc.put("nonlocal_org_sign", list.get(Integer.parseInt(environment.getProperty("jiesuan.nonlocal_org_sign"))));
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
        doc.put("area_code", list.get(Integer.parseInt(environment.getProperty("mingxi.area_code"))));
//        参保人统筹区代码
        doc.put("area_person_code", list.get(Integer.parseInt(environment.getProperty("mingxi.area_person_code"))));
//        定点机构编码
        doc.put("medical_code", list.get(Integer.parseInt(environment.getProperty("mingxi.medical_code"))));
//        定点机构名称
        doc.put("medical_name", list.get(Integer.parseInt(environment.getProperty("mingxi.medical_name"))));
//        社会保障卡号
        doc.put("social_card", list.get(Integer.parseInt(environment.getProperty("mingxi.social_card"))));
//        证件号码
        doc.put("card_id", list.get(Integer.parseInt(environment.getProperty("mingxi.card_id"))));
//        人员编号
        doc.put("psn_no", list.get(Integer.parseInt(environment.getProperty("mingxi.psn_no"))));
//        姓名
        doc.put("patient_name", list.get(Integer.parseInt(environment.getProperty("mingxi.patient_name"))));
//        险种类型
        doc.put("benefit_type", list.get(Integer.parseInt(environment.getProperty("mingxi.benefit_type"))));
//        医疗类别
        doc.put("medical_mode", list.get(Integer.parseInt(environment.getProperty("mingxi.medical_mode"))));
//        就诊号
        doc.put("eposide_id", list.get(Integer.parseInt(environment.getProperty("mingxi.eposide_id"))));
//        单据号
        doc.put("bill_id", list.get(Integer.parseInt(environment.getProperty("mingxi.bill_id"))));
//        单据明细号
        doc.put("bill_detail_id", list.get(Integer.parseInt(environment.getProperty("mingxi.bill_detail_id"))));
//         门诊或住院号
        doc.put("hospital_id", list.get(Integer.parseInt(environment.getProperty("mingxi.hospital_id"))));
//        费用发生时间
        doc.put("cost_time", list.get(Integer.parseInt(environment.getProperty("mingxi.cost_time"))));
//        费用结算时间
        doc.put("clear_time", list.get(Integer.parseInt(environment.getProperty("mingxi.clear_time"))));
//        医保目录编码
        doc.put("item_code", list.get(Integer.parseInt(environment.getProperty("mingxi.item_code"))));
//        医保目录名称
        doc.put("item_name", list.get(Integer.parseInt(environment.getProperty("mingxi.item_name"))));
//        机构收费项目编码
        doc.put("item_code_hosp", list.get(Integer.parseInt(environment.getProperty("mingxi.item_code_hosp"))));
//        机构收费项目名称
        doc.put("item_name_hosp", list.get(Integer.parseInt(environment.getProperty("mingxi.item_name_hosp"))));
//        收费项目类别
        doc.put("charge_type", list.get(Integer.parseInt(environment.getProperty("mingxi.charge_type"))));
//         费用类别
        doc.put("cost_type", list.get(Integer.parseInt(environment.getProperty("mingxi.cost_type"))));
//        单价
        Document unitPrice = pack("元", list.get(Integer.parseInt(environment.getProperty("mingxi.unit_price"))));
        doc.put("unit_price", unitPrice);
//        限价
        Document maxPrice = pack("元", list.get(Integer.parseInt(environment.getProperty("mingxi.max_price"))));
        doc.put("max_price", maxPrice);
//        帖数
        Document dose = pack("个", list.get(Integer.parseInt(environment.getProperty("mingxi.dose"))));
        doc.put("dose", dose);
//        数量
        Document num = pack("个", list.get(Integer.parseInt(environment.getProperty("mingxi.num"))));
        doc.put("num", num);
//        金额
        Document money = pack("元", list.get(Integer.parseInt(environment.getProperty("mingxi.money"))));
        doc.put("money", money);
//        自付比例
        Document payPerRetio = pack("比", list.get(Integer.parseInt(environment.getProperty("mingxi.pay_per_retio"))));
        doc.put("pay_per_retio", payPerRetio);
//        医保范围费用
        Document moneyMedical = pack("元", list.get(Integer.parseInt(environment.getProperty("mingxi.money_medical"))));
        doc.put("money_medical", moneyMedical);
//        自理费用
        Document moneySelfPay = pack("元", list.get(Integer.parseInt(environment.getProperty("mingxi.money_self_pay"))));
        doc.put("money_self_pay", moneySelfPay);
//        自费费用
        Document moneySelfOut = pack("元", list.get(Integer.parseInt(environment.getProperty("mingxi.money_self_out"))));
        doc.put("money_self_out", moneySelfOut);
//        剂型
        doc.put("dosage_form", list.get(Integer.parseInt(environment.getProperty("mingxi.dosage_form"))));
//        规格
        doc.put("spec", list.get(Integer.parseInt(environment.getProperty("mingxi.spec"))));
//        药品剂型单位
        doc.put("pack_unit", list.get(Integer.parseInt(environment.getProperty("mingxi.pack_unit"))));
//        生产企业
        doc.put("bus_produce", list.get(Integer.parseInt(environment.getProperty("mingxi.bus_produce"))));
//        药品包装转化比
        Document packRetio = pack("比", list.get(Integer.parseInt(environment.getProperty("mingxi.pack_retio"))));
        doc.put("pack_retio", packRetio);
//        特殊病种标识
        doc.put("is_special", list.get(Integer.parseInt(environment.getProperty("mingxi.is_special"))));
//        是否处方药
        doc.put("is_recipel", list.get(Integer.parseInt(environment.getProperty("mingxi.is_recipel"))));
//        单复方标志
        doc.put("is_single", list.get(Integer.parseInt(environment.getProperty("mingxi.is_single"))));
//        处方号
        doc.put("recipel_no", list.get(Integer.parseInt(environment.getProperty("mingxi.recipel_no"))));
//        科室名称
        doc.put("dept_name", list.get(Integer.parseInt(environment.getProperty("mingxi.dept_name"))));
//        执行科室名称
        doc.put("discharge_dept_name", list.get(Integer.parseInt(environment.getProperty("mingxi.discharge_dept_name"))));
//        医生编码
        doc.put("doctor_code", list.get(Integer.parseInt(environment.getProperty("mingxi.doctor_code"))));
//       医生姓名
        doc.put("doctor_name", list.get(Integer.parseInt(environment.getProperty("mingxi.doctor_name"))));

        return doc;
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
