package com.example.service;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.InsertManyOptions;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.RequestParam;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import static com.mongodb.WriteConcern.ACKNOWLEDGED;
import static java.lang.Math.floor;

@Service
public class ImportService {
    private static int amountData = 0;

    @Value("${spring.data.mongodb.port:}")
    private String port;
    @Value("${spring.data.mongodb.host:}")
    private String host;

    /**
     * 原始三张表
     *
     * @param collectionName
     * @param target
     */
    public String importsDataOpt(String collectionName, String target, String filepath) {
        //String filepath = "D:\\桌面\\第一人民门诊明细.csv";
        //转换数据方便数据分发
        List<String> csvData = dataExtraction(filepath, target);
        //数据分发
        List<List<String>> csvDataList = dataDistribution(csvData);
        CountDownLatch latch = new CountDownLatch(8);
        //开启线程
        for (int i = 0; i < 8; i++) {
            int finalI = i;
            new Thread(() -> {
                read(csvDataList.get(finalI), collectionName, target, finalI);
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
     * @param filepath
     * @param target
     * @return
     */
    private List<String> dataExtraction(String filepath, String target) {
        int size = 0;
        switch (target) {
            case "zhenduan":
                size = 11;
                break;
            case "jiesuan":
                size = 36;
                break;
            case "mingxi":
                size = 44;
                break;
        }
        try {
            BufferedReader br = new BufferedReader(new FileReader(filepath), 81920);
            String line;
            List<String> list = new ArrayList<>();
            List<String> joint = new ArrayList<>();
            String record = null;
            line = br.readLine();
            while ((line = br.readLine()) != null) {
                line = line + ",1";
                line = line.replace("\"", "");
                //判断长度 看是否换行拼接下一行逻辑
                if (joint.isEmpty()) {
                    joint = Arrays.asList(line.split(","));
                    record = line;
                    record = record.substring(0, record.length() - 2);
                    if (joint.size() >= size + 1) {
                        joint = new ArrayList<>();
                        list.add(line);
                    }
                } else {
                    record += line;
                    joint = new ArrayList<>();
                    list.add(record);
                }
            }
            br.close();
            return list;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * @param csvData
     * @return
     */
    public List<List<String>> dataDistribution(List<String> csvData) {
        List<List<String>> distributionData = new ArrayList<>();
        int size = csvData.size();

        //计算各个线程可以整块分发的最大数据量
        amountData = (int) floor(size / 8);
        List a = new ArrayList<>();
        int end = 0;
        for (Integer i = 0; i < 8; i++) {
            List<String> data = new ArrayList<>();
            int begin = (int) (i * amountData);
            end = (int) ((i + 1) * amountData);
            if (i == 7)
                data = csvData.subList(begin, size);
            else
                data = csvData.subList(begin, end);
            distributionData.add(data);
        }
        return distributionData;
    }

    /**
     * @param csvData
     * @param collectionName
     * @param target
     * @param theadId
     */
    public void read(List<String> csvData, String collectionName, String target, int theadId) {

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
            for (String csvDatum : csvData) {
                List<String> list = new ArrayList<>();
                Document doc = new Document();
                list = Arrays.asList(csvDatum.split(","));
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
                        doc = zhenduanfield(list, doc);
                        break;
                    case "jiesuan":
                        doc = jiesuanfield(list, doc);
                        break;
                    case "mingxi":
                        doc = mingxifield(list, doc);
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
