package com.example.controller;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.InsertManyOptions;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.*;

import java.io.*;
import java.util.*;

import static com.mongodb.WriteConcern.ACKNOWLEDGED;
import static java.lang.Math.floor;

@RequestMapping("import")
@RestController
public class Import {
    //多线程
    @GetMapping("csv")
    public void importsDataOpt(@RequestParam(required = true, defaultValue = "") String port,
                               @RequestParam(required = true, defaultValue = "") String host) {
        String filepath = "D:\\桌面\\第一人民门诊明细.csv";
        //转换数据方便数据分发
        List<String> csvData = dataExtraction(filepath);
        //数据分发
        List<List<String>> csvDataList = dataDistribution(csvData);

        for (int i = 0; i < 8; i++) {
            int finalI = i;
            new Thread(() -> {
                read(port, csvDataList.get(finalI), host);
            }).start();
        }
    }


    //转换数据
    private static List<String> dataExtraction(String filepath) {
        try {
            BufferedReader br = new BufferedReader(new FileReader(filepath));
            String line;
            List<String> list = new ArrayList<>();
            while ((line = br.readLine()) != null) {
                line = line + ",1";
                line = line.replace("\"","");
                //加判断长度 看是否换行拼接下一行逻辑
                list.add(line);
            }
            br.close();
            return list;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public List<List<String>> dataDistribution(List<String> csvData) {
        List<List<String>> distributionData = new ArrayList<>();
        int size = csvData.size();

        //计算各个线程可以整块分发的最大数据量
        int amountData = (int) floor(size / 8);
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

    public static void read(String port, List<String> csvData, String host) {
        String connectionString = "mongodb://" + host + ":" + port;
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
        String collectionName = "com.ns.entity.object.form.instance.ns3g475kl6jj2eb4ixfi.yibaozhenduanxinxi_1";
        try (MongoClient mongoClient = MongoClients.create(settings)) {
            MongoDatabase database = mongoClient.getDatabase(dbName);
            MongoCollection<Document> collection = database.getCollection(collectionName);
            List<Document> batchDocuments = new ArrayList<>();

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

}

