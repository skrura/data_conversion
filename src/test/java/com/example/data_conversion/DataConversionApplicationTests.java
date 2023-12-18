package com.example.data_conversion;


import cn.hutool.core.io.FileUtil;
import cn.hutool.core.io.IoUtil;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.shadow.com.univocity.parsers.csv.CsvFormat;
import org.junit.jupiter.params.shadow.com.univocity.parsers.csv.CsvParser;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.aggregation.TypedAggregation;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.query.*;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import sun.management.resources.agent;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.Math.floor;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
//@ContextConfiguration(classes =  DataConversionApplicationTests.class)
class DataConversionApplicationTests {

    @Autowired(required = false)
    private MongoTemplate mongoTemplate;

    @Test
    void zhenduanxinxi_zhuyuan() throws ParseException {
        //数据列表
        List<Map> zhuyuanzhubiao = new ArrayList<>();
        //时间转换
        //SimpleDateFormat ymd = new SimpleDateFormat("yyyy-MM-dd");

        String zhuyuanzhubiaoku = "com.ns.entity.object.form.instance.nsjgpv79iu55j528ig78.xiaoganshidiyirenminyiyuanzhuyuanzhudan_2";
        String menzhenzhubiaoku = "com.ns.entity.object.form.instance.nsjgpv79iu55j528ig78.xiaoganshidiyirenminyiyuanmenzhenzhudan_1";
        String zhenduanxinxiku = "com.ns.entity.object.form.instance.nsjgpv79iu55j528ig78.zhenduanxinxi_2";

        //    Map shu = mongoTemplate.findOne(new Query(Criteria.where("jiesuandanjuhao_3").is("5150667")), Map.class, zhuyuanzhubiaoku);

        zhuyuanzhubiao = mongoTemplate.findAll(Map.class, zhuyuanzhubiaoku);

        String empty = "";
        //入库
        List<Map<String, Object>> insertList = new ArrayList<>();
        for (Map shu : zhuyuanzhubiao) {
            //封装数据
            Map<String, Object> map = new HashMap<>();
            Map<String, Object> map0 = new HashMap<>();
            //系统字段  一条住院单分为两个诊断信息单   map住院  map0出院
            String mainId = UUID.randomUUID().toString();
            map.put("_id", mainId);
            map.put("create_time", System.currentTimeMillis());
            map.put("create_account", "admin");
            map.put("category_id", "c1535842-d51f-4991-8c72-8b976ef1331e");
            map.put("data_status", "已归档");
            map.put("data_type", 1);
            map.put("priority", "");
            map.put("bind_id", mainId);
            map.put("corp_id", "nsrcu88p7uy22m7i9ioz");
            map.put("parent_corp_id_list", new ArrayList<>());
            map.put("bind_category_id", "");

            String string = UUID.randomUUID().toString();
            map0.put("_id", string);
            map0.put("create_time", System.currentTimeMillis());
            map0.put("create_account", "admin");
            map0.put("category_id", "c1535842-d51f-4991-8c72-8b976ef1331e");
            map0.put("data_status", "已归档");
            map0.put("data_type", 1);
            map0.put("priority", "");
            map0.put("bind_id", string);
            map0.put("corp_id", "nsrcu88p7uy22m7i9ioz");
            map0.put("parent_corp_id_list", new ArrayList<>());
            map0.put("bind_category_id", "");

            //业务字段
            map.put("zhenduanxinxiID_1", empty);
            map.put("jiuzhenhao_17", shu.get("jiesuandanjuhao_3"));
            map.put("renyuanbianhao_4", shu.get("gerenbianma_4"));
            map.put("churuyuanzhenduanleibie_2", "2");
            map.put("zhenduanleibie_4", empty);
            map.put("zhuzhenduanbiaozhi_2", empty);
            map.put("zhenduanpaixuhao_2", empty);
            map.put("zhenduandaima_2", shu.get("ruyuanzhenduanbianma_3"));
            map.put("zhenduanmingcheng_7", shu.get("ruyuanzhenduanmingcheng_2"));
            map.put("ruyuanbingqing_5", empty);
            map.put("zhenduankeshi_1", shu.get("ruyuankeshimingcheng_4"));
            map.put("zhenduanyishidaima_1", shu.get("yishengbianhao_2"));
            map.put("zhenduanyishixingming_1", shu.get("yishengxingming_6"));
            map.put("zhenduanshijian_4", shu.get("ruyuanriqi_8"));


            //业务字段
            map0.put("zhenduanxinxiID_1", empty);
            map0.put("jiuzhenhao_17", shu.get("jiesuandanjuhao_3"));
            map0.put("renyuanbianhao_4", shu.get("gerenbianma_4"));
            map0.put("churuyuanzhenduanleibie_2", "3");
            map0.put("zhenduanleibie_4", empty);
            map0.put("zhuzhenduanbiaozhi_2", empty);
            map0.put("zhenduanpaixuhao_2", empty);
            map0.put("zhenduandaima_2", shu.get("chuyuanzhenduanbianma_2"));
            map0.put("zhenduanmingcheng_7", shu.get("chuyuanzhenduanmingcheng_2"));
            map0.put("ruyuanbingqing_5", empty);
            map0.put("zhenduankeshi_1", shu.get("chuyuankeshimingcheng_3"));
            map0.put("zhenduanyishidaima_1", shu.get("yishengbianhao_2"));
            map0.put("zhenduanyishixingming_1", shu.get("yishengxingming_6"));
            map0.put("zhenduanshijian_4", shu.get("ruyuanriqi_8"));
            insertList.add(map);
            insertList.add(map0);
            if (insertList.size() >= 1500) {
                mongoTemplate.insert(insertList, zhenduanxinxiku);
                insertList = new ArrayList<>();
                //System.out.println(map.get("churuyuanzhenduanleibie_2"));
            }
        }
        if (insertList.size() > 0) {
            mongoTemplate.insert(insertList, zhenduanxinxiku);
            insertList = new ArrayList<>();
        }


    }

    @Test
    void zhneduanxinx_menzhen() {
        List<Map> menzhenzhubiao = new ArrayList<>();

        String menzhenzhubiaoku = "com.ns.entity.object.form.instance.nsjgpv79iu55j528ig78.xiaoganshidiyirenminyiyuanmenzhenzhudan_1";
        String zhenduanxinxiku = "com.ns.entity.object.form.instance.nsjgpv79iu55j528ig78.zhenduanxinxi_2";
        String empty = "";
        List<Map<String, Object>> insertList = new ArrayList<>();
        menzhenzhubiao = mongoTemplate.findAll(Map.class, menzhenzhubiaoku);
        for (Map shu : menzhenzhubiao) {
            Map<String, Object> map = new HashMap<>();
            //系统字段
            String mainId = UUID.randomUUID().toString();
            map.put("_id", mainId);
            map.put("create_time", System.currentTimeMillis());
            map.put("create_account", "admin");
            map.put("category_id", "c1535842-d51f-4991-8c72-8b976ef1331e");
            map.put("data_status", "已归档");
            map.put("data_type", 1);
            map.put("priority", "");
            map.put("bind_id", mainId);
            map.put("corp_id", "nsrcu88p7uy22m7i9ioz");
            map.put("parent_corp_id_list", new ArrayList<>());
            map.put("bind_category_id", "");

            //业务字段
            map.put("zhenduanxinxiID_1", empty);
            map.put("jiuzhenhao_17", shu.get("yibaojiesuandanjuhao_1"));
            map.put("renyuanbianhao_4", shu.get("gerenbianma_5"));
            map.put("churuyuanzhenduanleibie_2", "1");
            map.put("zhenduanleibie_4", empty);
            map.put("zhuzhenduanbiaozhi_2", empty);
            map.put("zhenduanpaixuhao_2", empty);
            map.put("zhenduandaima_2", shu.get("zhenduanbianma_4"));
            map.put("zhenduanmingcheng_7", shu.get("zhenduanmingcheng_4"));
            map.put("ruyuanbingqing_5", empty);
            map.put("zhenduankeshi_1", shu.get("keshimingcheng_24"));
            map.put("zhenduanyishidaima_1", shu.get("zhuzhenyishibianma_2"));
            map.put("zhenduanyishixingming_1", shu.get("zhuzhenyishimingcheng_2"));
            map.put("zhenduanshijian_4", shu.get("jiesuanriqi_8"));
            insertList.add(map);

            if (insertList.size() >= 1500) {
                mongoTemplate.insert(insertList, zhenduanxinxiku);
                insertList = new ArrayList<>();
            }
        }
        if (insertList.size() > 0) {
            mongoTemplate.insert(insertList, zhenduanxinxiku);
            insertList = new ArrayList<>();
        }
    }

    @Test
    void feiyongjiesuanclear() {
        String feiyongjiesuanku = "com.ns.entity.object.form.instance.nsjgpv79iu55j528ig78.feiyongjiesuanxinxi_1";
        List<Map> all = mongoTemplate.findAll(Map.class, feiyongjiesuanku);
        if (all.size() > 0) {
            mongoTemplate.remove(new Query(), feiyongjiesuanku);
            // System.out.println(all);
        }
    }

    @Test
    void feiyongjiesuancount() {
        String feiyongjiesuanku = "com.ns.entity.object.form.instance.nsjgpv79iu55j528ig78.feiyongjiesuanxinxi_1";

        String huanzhebiaoku = "com.ns.entity.object.form.instance.nsjgpv79iu55j528ig78.huanzhebiao_1";
        String jiesuanku = "com.ns.entity.object.form.instance.nsjgpv79iu55j528ig78.feiyongjiesuanxinxi_1";
        String mingxiku = "com.ns.entity.object.form.instance.nsjgpv79iu55j528ig78.feiyongmingxixinxi_1";


        System.out.println("患者：" + mongoTemplate.count(new Query(), huanzhebiaoku));
    }

    @Test
    void feiyongjiesuan_zhuyuan() {
        //数据列表
        List<Map> zhuyuanzhubiao = new ArrayList<>();

        String zhuyuanzhubiaoku = "com.ns.entity.object.form.instance.nsjgpv79iu55j528ig78.xiaoganshidiyirenminyiyuanzhuyuanzhudan_2";
        String feiyongjiesuanku = "com.ns.entity.object.form.instance.nsjgpv79iu55j528ig78.feiyongjiesuanxinxi_1";

        zhuyuanzhubiao = mongoTemplate.findAll(Map.class, zhuyuanzhubiaoku);

        //Map shu = mongoTemplate.findOne(new Query(Criteria.where("jiesuandanjuhao_3").is("5150667")), Map.class, zhuyuanzhubiaoku);
        String empty = "";

        //入库
        List<Map<String, Object>> insertList = new ArrayList<>();

        for (Map shu : zhuyuanzhubiao) {
            //封装数据
            Map<String, Object> map = new HashMap<>();
            //系统字段
            String mainId = UUID.randomUUID().toString();
            map.put("_id", mainId);
            map.put("create_time", System.currentTimeMillis());
            map.put("create_account", "admin");
            map.put("category_id", "ba6ae22a-73ad-4375-95ae-ede2de46c916");
            map.put("data_status", "已归档");
            map.put("data_type", 1);
            map.put("priority", "");
            map.put("bind_id", mainId);
            map.put("corp_id", "nsrcu88p7uy22m7i9ioz");
            map.put("parent_corp_id_list", new ArrayList<>());
            map.put("bind_category_id", "");

            //业务字段
            map.put("tongchouqumingcheng_5", empty);
            map.put("canbaorentongchouqumingcheng_5", empty);
            map.put("yibaoniandu_5", empty);
            map.put("dingdianjigoubianma_5", shu.get("yiliaojigoubianma_8"));
            map.put("dingdianjigoumingcheng_5", shu.get("yiliaojigoumingcheng_8"));
            map.put("shehuibaozhangkahao_5", empty);
            map.put("zhengjianhaoma_22", empty);
            map.put("renyuanbianhao_2", shu.get("gerenbianma_4"));
            map.put("xingming_103", shu.get("huanzhexingming_6"));
            map.put("chushengriqi_28", shu.get("huanzhechushengriqi_3"));
            map.put("nianling_23", shu.get("huanzhenianling_3"));
            map.put("xingbie_22", shu.get("huanzhexingbie_4"));
            map.put("danweimingcheng_25", shu.get("canbaorendanweidizhi_2"));
            map.put("jiatingzhuzhi_6", empty);
            map.put("lianxidianhua_26", empty);
            map.put("renyuanleibie_8", empty);
            map.put("xianzhongleixing_2", shu.get("xianzhongleixing_6"));
            map.put("yiliaoleibie_8", "住院");
            map.put("jiuzhenhao_15", empty);
            map.put("danjuhao_12", shu.get("jiesuandanjuhao_3"));
            map.put("binganshouyehao_3", empty);
            map.put("feiyongjiesuanshijian_5", shu.get("jiesuanriqi_7"));
            map.put("yiliaofeizonge_6", shu.get("zongfeiyong_3"));

            Map<String, Object> zhifuMoney = new HashMap<>();
            zhifuMoney.put("unit", "元");
            zhifuMoney.put("value", 0.0);

            map.put("gerenzhanghuzhifu_8", zhifuMoney);

            Map<String, Object> xianjinMoney = new HashMap<>();
            xianjinMoney.put("unit", "元");
            xianjinMoney.put("value", 0.0);

            map.put("gerenxianjinzhifu_7", xianjinMoney);

            map.put("tongchouzhifujine_5", shu.get("jibentongchouzhifu_2"));

            Map<String, Object> dabingMoney = new HashMap<>();
            dabingMoney.put("unit", "元");
            dabingMoney.put("value", 0.0);

            map.put("dabingzhifu_3", dabingMoney);

            Map<String, Object> gongwuMoney = new HashMap<>();
            gongwuMoney.put("unit", "元");
            gongwuMoney.put("value", 0.0);

            map.put("gongwuyuanbuzhu_3", gongwuMoney);

            Map<String, Object> minzhengMoney = new HashMap<>();
            minzhengMoney.put("unit", "元");
            minzhengMoney.put("value", 0.0);

            map.put("minzhengjijin_3", minzhengMoney);

            Map<String, Object> canlianMoney = new HashMap<>();
            canlianMoney.put("unit", "元");
            canlianMoney.put("value", 0.0);

            map.put("canlianjijin_3", canlianMoney);

            Map<String, Object> qitaMoney = new HashMap<>();
            qitaMoney.put("unit", "元");
            qitaMoney.put("value", 0.0);

            map.put("qitabuzhu_3", qitaMoney);

            Map<String, Object> jiatingMoney = new HashMap<>();
            jiatingMoney.put("unit", "元");
            jiatingMoney.put("value", 0.0);

            map.put("jiatinggongjizhanghuzhifu_3", jiatingMoney);

            Map<String, Object> dangnianMoney = new HashMap<>();
            dangnianMoney.put("unit", "元");
            dangnianMoney.put("value", 0.0);

            map.put("dangnianzhanghuzhifue_3", dangnianMoney);

            Map<String, Object> linianMoney = new HashMap<>();
            linianMoney.put("unit", "元");
            linianMoney.put("value", 0.0);

            map.put("linianzhanghuzhifue_3", linianMoney);

            Map<String, Object> zifeiMoney = new HashMap<>();
            zifeiMoney.put("unit", "元");
            zifeiMoney.put("value", 0.0);

            map.put("gerenzifei_3", zifeiMoney);


            Map<String, Object> ziliMoney = new HashMap<>();
            ziliMoney.put("unit", "元");
            ziliMoney.put("value", 0.0);

            map.put("gerenzili_1", ziliMoney);

            Map<String, Object> zifuMoney = new HashMap<>();
            zifuMoney.put("unit", "元");
            zifuMoney.put("value", 0.0);

            map.put("gerenzifu_5", zifuMoney);

            Map<String, Object> jiesuanqiandangMoney = new HashMap<>();
            jiesuanqiandangMoney.put("unit", "元");
            jiesuanqiandangMoney.put("value", 0.0);

            map.put("jiesuanqiandangnianzhanghuyue_3", jiesuanqiandangMoney);

            Map<String, Object> jiesuanqianliMoney = new HashMap<>();
            jiesuanqianliMoney.put("unit", "元");
            jiesuanqianliMoney.put("value", 0.0);

            map.put("jiesuanqianlinianzhanghuyue_3", jiesuanqianliMoney);


            map.put("teshubingzhongbiaoshi_5", empty);
            map.put("jiesuanzhuangtai_7", empty);
            map.put("jiesuanfangshi_21", empty);
            map.put("yibaofufeifangshi_3", empty);
            map.put("yiliaofeiyongzhifufangshi_3", empty);
            map.put("fapiaohaoma_12", empty);
            map.put("chuangweihao_5", shu.get("zhuyuanchuangweihao_2"));
            map.put("ruyuanriqi_2", shu.get("ruyuanriqi_8"));
            map.put("chuyuanriqi_2", shu.get("chuyuanriqi_8"));
            map.put("zhuyuantianshu_3", shu.get("zhuyuantianshu_7"));
            map.put("ruyuanzhenduanjibingbianma_3", shu.get("ruyuanzhenduanbianma_3"));
            map.put("ruyuanjibingmingcheng_3", shu.get("ruyuanzhenduanmingcheng_2"));
            map.put("chuyuanjibingzhenduanbianma_3", shu.get("chuyuanzhenduanbianma_2"));
            map.put("chuyuanjibingmingcheng_3", shu.get("chuyuanzhenduanmingcheng_2"));
            map.put("ruyuankeshimingcheng_2", shu.get("ruyuankeshimingcheng_4"));
            map.put("chuyuankeshimingcheng_2", shu.get("chuyuankeshimingcheng_3"));
            map.put("zhuzhiyishengdaima_1", shu.get("yishengbianhao_2"));
            map.put("zhuzhiyisheng_1", shu.get("yishengxingming_6"));
            map.put("liyuanfangshi_7", empty);
            map.put("chongxiaodanjuhao_3", empty);
            map.put("yidijiuzhenbiaozhi_1", empty);

            insertList.add(map);

            if (insertList.size() >= 1500) {
                mongoTemplate.insert(insertList, feiyongjiesuanku);
                insertList = new ArrayList<>();
                // System.out.println(map);
            }
        }
        if (insertList.size() > 0) {
            mongoTemplate.insert(insertList, feiyongjiesuanku);
            insertList = new ArrayList<>();
            //System.out.println(map);
        }
    }

    @Test
    void feiyongjiesuan_menzhen() {
        List<Map> menzhenzhubiao = new ArrayList<>();

        String menzhenzhubiaoku = "com.ns.entity.object.form.instance.nsjgpv79iu55j528ig78.xiaoganshidiyirenminyiyuanmenzhenzhudan_1";
        String feiyongjiesuanku = "com.ns.entity.object.form.instance.nsjgpv79iu55j528ig78.feiyongjiesuanxinxi_1";

        //Map shu = mongoTemplate.findOne(new Query(Criteria.where("yibaojiesuandanjuhao_1").is("400z001")), Map.class, menzhenzhubiaoku);
        menzhenzhubiao = mongoTemplate.findAll(Map.class, menzhenzhubiaoku);
        String empty = "";

        //入库
        List<Map<String, Object>> insertList = new ArrayList<>();
        for (Map shu : menzhenzhubiao) {
            //封装数据
            Map<String, Object> map = new HashMap<>();
            //系统字段
            String mainId = UUID.randomUUID().toString();
            map.put("_id", mainId);
            map.put("create_time", System.currentTimeMillis());
            map.put("create_account", "admin");
            map.put("category_id", "ba6ae22a-73ad-4375-95ae-ede2de46c916");
            map.put("data_status", "已归档");
            map.put("data_type", 1);
            map.put("priority", "");
            map.put("bind_id", mainId);
            map.put("corp_id", "nsrcu88p7uy22m7i9ioz");
            map.put("parent_corp_id_list", new ArrayList<>());
            map.put("bind_category_id", "");

            //业务字段
            map.put("tongchouqumingcheng_5", empty);
            map.put("canbaorentongchouqumingcheng_5", empty);
            map.put("yibaoniandu_5", empty);
            map.put("dingdianjigoubianma_5", shu.get("yiliaojigoubianma_9"));
            map.put("dingdianjigoumingcheng_5", shu.get("yiliaojigoumingcheng_9"));
            map.put("shehuibaozhangkahao_5", empty);
            map.put("zhengjianhaoma_22", empty);
            map.put("renyuanbianhao_2", shu.get("gerenbianma_5"));
            map.put("xingming_103", shu.get("huanzhexingming_7"));
            map.put("chushengriqi_28", shu.get("huanzhechushengriqi_4"));
            map.put("nianling_23", shu.get("huanzhenianling_4"));
            map.put("xingbie_22", shu.get("huanzhexingbie_5"));
            map.put("danweimingcheng_25", empty);
            map.put("jiatingzhuzhi_6", empty);
            map.put("lianxidianhua_26", empty);
            map.put("renyuanleibie_8", empty);
            map.put("xianzhongleixing_2", shu.get("jiesuanleibie_2"));
            map.put("yiliaoleibie_8", "普通门诊");
            map.put("jiuzhenhao_15", empty);
            map.put("danjuhao_12", shu.get("yibaojiesuandanjuhao_1"));
            map.put("binganshouyehao_3", empty);
            map.put("feiyongjiesuanshijian_5", shu.get("jiesuanriqi_8"));
            map.put("yiliaofeizonge_6", shu.get("yiliaozongfashengfeiyong_2"));
            map.put("gerenzhanghuzhifu_8", shu.get("gerenzhanghuzhifu_6"));

            Map<String, Object> xianjinMoney = new HashMap<>();
            xianjinMoney.put("unit", "元");
            xianjinMoney.put("value", 0.0);

            map.put("gerenxianjinzhifu_7", xianjinMoney);


            map.put("tongchouzhifujine_5", shu.get("yibaotongchoujijinzhifufeiyong_2"));

            Map<String, Object> dabingMoney = new HashMap<>();
            dabingMoney.put("unit", "元");
            dabingMoney.put("value", 0.0);

            map.put("dabingzhifu_3", dabingMoney);

            Map<String, Object> gongwuMoney = new HashMap<>();
            gongwuMoney.put("unit", "元");
            gongwuMoney.put("value", 0.0);

            map.put("gongwuyuanbuzhu_3", gongwuMoney);

            Map<String, Object> minzhengMoney = new HashMap<>();
            minzhengMoney.put("unit", "元");
            minzhengMoney.put("value", 0.0);

            map.put("minzhengjijin_3", minzhengMoney);

            Map<String, Object> canlianMoney = new HashMap<>();
            canlianMoney.put("unit", "元");
            canlianMoney.put("value", 0.0);

            map.put("canlianjijin_3", canlianMoney);

            Map<String, Object> qitaMoney = new HashMap<>();
            qitaMoney.put("unit", "元");
            qitaMoney.put("value", 0.0);

            map.put("qitabuzhu_3", qitaMoney);

            Map<String, Object> jiatingMoney = new HashMap<>();
            jiatingMoney.put("unit", "元");
            jiatingMoney.put("value", 0.0);

            map.put("jiatinggongjizhanghuzhifu_3", jiatingMoney);

            Map<String, Object> dangnianMoney = new HashMap<>();
            dangnianMoney.put("unit", "元");
            dangnianMoney.put("value", 0.0);

            map.put("dangnianzhanghuzhifue_3", dangnianMoney);

            Map<String, Object> linianMoney = new HashMap<>();
            linianMoney.put("unit", "元");
            linianMoney.put("value", 0.0);

            map.put("linianzhanghuzhifue_3", linianMoney);

            Map<String, Object> zifeiMoney = new HashMap<>();
            zifeiMoney.put("unit", "元");
            zifeiMoney.put("value", 0.0);

            map.put("gerenzifei_3", zifeiMoney);

            Map<String, Object> ziliMoney = new HashMap<>();
            ziliMoney.put("unit", "元");
            ziliMoney.put("value", 0.0);

            map.put("gerenzili_1", ziliMoney);

            Map<String, Object> zifuMoney = new HashMap<>();
            zifuMoney.put("unit", "元");
            zifuMoney.put("value", 0.0);

            map.put("gerenzifu_5", zifuMoney);

            Map<String, Object> jiesuanqiandangMoney = new HashMap<>();
            jiesuanqiandangMoney.put("unit", "元");
            jiesuanqiandangMoney.put("value", 0.0);

            map.put("jiesuanqiandangnianzhanghuyue_3", jiesuanqiandangMoney);

            Map<String, Object> jiesuanqianliMoney = new HashMap<>();
            jiesuanqianliMoney.put("unit", "元");
            jiesuanqianliMoney.put("value", 0.0);

            map.put("jiesuanqianlinianzhanghuyue_3", jiesuanqianliMoney);

            map.put("teshubingzhongbiaoshi_5", empty);
            map.put("jiesuanzhuangtai_7", empty);
            map.put("jiesuanfangshi_21", empty);
            map.put("yibaofufeifangshi_3", empty);
            map.put("yiliaofeiyongzhifufangshi_3", empty);
            map.put("fapiaohaoma_12", empty);
            map.put("chuangweihao_5", empty);
            map.put("ruyuanriqi_2", empty);
            map.put("chuyuanriqi_2", empty);

            Map<String, Object> zhuyuantianshu = new HashMap<>();
            zhuyuantianshu.put("unit", "天");
            zhuyuantianshu.put("value", 0);

            map.put("zhuyuantianshu_3", zhuyuantianshu);

            map.put("ruyuanzhenduanjibingbianma_3", shu.get("zhenduanbianma_4"));
            map.put("ruyuanjibingmingcheng_3", shu.get("zhenduanmingcheng_4"));
            map.put("chuyuanjibingzhenduanbianma_3", empty);
            map.put("chuyuanjibingmingcheng_3", empty);
            map.put("ruyuankeshimingcheng_2", shu.get("keshimingcheng_24"));
            map.put("chuyuankeshimingcheng_2", empty);
            map.put("zhuzhiyishengdaima_1", shu.get("zhuzhenyishibianma_2"));
            map.put("zhuzhiyisheng_1", shu.get("zhuzhenyishimingcheng_2"));
            map.put("liyuanfangshi_7", empty);
            map.put("chongxiaodanjuhao_3", empty);
            map.put("yidijiuzhenbiaozhi_1", empty);

            insertList.add(map);

            if (insertList.size() >= 1500) {
                mongoTemplate.insert(insertList, feiyongjiesuanku);
                insertList = new ArrayList<>();
                //   System.out.println(map);
            }
        }
        if (insertList.size() > 0) {
            mongoTemplate.insert(insertList, feiyongjiesuanku);
            insertList = new ArrayList<>();
        }
    }

    @Test
    void yishengbiaoclear() {
        String yishengbiao = "com.ns.entity.object.form.instance.nsjgpv79iu55j528ig78.yishengbiao_1";
        List<Map> all = mongoTemplate.findAll(Map.class, yishengbiao);
        if (all.size() > 0) {
            mongoTemplate.remove(new Query(), yishengbiao);
            // System.out.println(all);
        }
    }

    @Test
    public Map<String, Object> mapStringToMap(String str) {
        str = str.substring(1, str.length() - 1);
        String[] strs = str.split(",");
        Map<String, Object> map = new HashMap<String, Object>();
        for (String string : strs) {
            String key = string.split("=")[0];
            Object value = string.split("=")[1];
            // 去掉头部空格
            String key1 = key.trim();
            Object value1 = value.toString().trim();
            map.put(key1, value1);
        }
        return map;
    }


    @Test
    void yishengbiaodaoru() {

        List<Map> mingxiyisheng = new ArrayList<>();
        Map<String, List<Map>> fenliemingxi = new HashMap<>();

        String yishengbiaoku = "com.ns.entity.object.form.instance.nsjgpv79iu55j528ig78.yishengbiao_1";
        String jiesuanku = "com.ns.entity.object.form.instance.nsjgpv79iu55j528ig78.feiyongjiesuanxinxi_1";
        String mingxiku = "com.ns.entity.object.form.instance.nsjgpv79iu55j528ig78.feiyongmingxixinxi_1";

        Query q1 = new Query();
        q1.fields().include("zhuzhiyishengdaima_1").exclude("_id");
        List<Map> one = new ArrayList<>();
        one = mongoTemplate.find(q1, Map.class, jiesuanku);
        Set<Map> yibianma = new HashSet<>(one);

        Query q2 = new Query();

        q2.fields().include("yiliaoleibie_8").include("tongchouzhifujine_5")
                .include("yiliaofeizonge_6").include("xingming_103")
                .include("zhuzhiyishengdaima_1").include("ruyuankeshimingcheng_2")
                .include("zhuzhiyisheng_1").exclude("_id");

        List<Map> jieyisheng = mongoTemplate.find(q2, Map.class, jiesuanku);

        Map<String, List<Map>> fenliejiesuan = new HashMap<>();
        for (Map k : jieyisheng) {
            String fieldValue = k.get("zhuzhiyishengdaima_1").toString(); // 假设用于分组的字段名为 "fieldName"
            if (fenliejiesuan.get(fieldValue) == null) {
                fenliejiesuan.put(fieldValue, new ArrayList<>());
            }

            fenliejiesuan.get(fieldValue).add(k);
        }

        Query q3 = new Query();
        q3.fields().include("jine_20").include("shoufeixiangmuleibie_5").include("xingming_103")
                .include("yishengbianma_5").exclude("_id");


        for (int s = 0; s <= 500; s++) {
            mingxiyisheng = mongoTemplate.find(q3.skip(s * 3250).limit(3250), Map.class, mingxiku);
            if (mingxiyisheng.size() == 0) {
                break;
            }
            for (Map k : mingxiyisheng) {
                String fieldValue = k.get("yishengbianma_5").toString(); // 假设用于分组的字段名为 "fieldName"
                if (fenliemingxi.get(fieldValue) == null) {
                    fenliemingxi.put(fieldValue, new ArrayList<>());
                }
                fenliemingxi.get(fieldValue).add(k);
            }
            System.out.println(s);
            mingxiyisheng.clear();
        }


        //  List<Map<String, Object>> insertList = new ArrayList<>();

        for (Map mapstrjie : yibianma) {
            String jie = mapstrjie.get("zhuzhiyishengdaima_1").toString();
            //   String daima = jie.get("zhuzhiyishengdaima_1").toString();

            Map<String, Object> map = new HashMap<>();
            //系统字段
            String mainId = UUID.randomUUID().toString();
            map.put("_id", mainId);
            map.put("create_time", System.currentTimeMillis());
            map.put("create_account", "admin");
            map.put("category_id", "b5ed0514-2745-4452-9a83-69cb4a5cfab4");
            map.put("data_status", "已归档");
            map.put("data_type", 1);
            map.put("priority", "");
            map.put("bind_id", mainId);
            map.put("corp_id", "nsrcu88p7uy22m7i9ioz");
            map.put("parent_corp_id_list", new ArrayList<>());
            map.put("bind_category_id", "");


            //业务字段
            map.put("yiyuanmingcheng_8", "孝感市第一人民医院");
            map.put("keshimingcheng_30", fenliejiesuan.get(jie).get(0).get("ruyuankeshimingcheng_2"));
            map.put("yishengbianma_9", jie);

            map.put("yishengmingcheng_1", fenliejiesuan.get(jie).get(0).get("zhuzhiyisheng_1"));

            map.put("yibaoniandu_8", "");
            map.put("yiyuanleibie_6", "公立医院");
            map.put("yiyuandengji_6", "三级");
            map.put("yiyuanxingzhi_6", "");


            Map<String, Object> yibaoMap = new HashMap<>();
            yibaoMap.put("unit", "元");
            Double yibaomoney = 0.0;


            for (Map k : fenliejiesuan.get(jie)) {
                yibaomoney += Double.parseDouble(mapStringToMap(k.get("tongchouzhifujine_5").toString()).get("value").toString());
            }

            yibaoMap.put("value", yibaomoney);
            map.put("yibaofanweifeiyong_7", yibaoMap);

            ArrayList<String> jiancha = new ArrayList<>();

            jiancha.add("CT费");
            jiancha.add("TCD");
            jiancha.add("彩超费");
            jiancha.add("磁共振");
            jiancha.add("化验费");
            jiancha.add("检查费");
            jiancha.add("脑电图");
            jiancha.add("拍片费");
            jiancha.add("胃镜费");
            jiancha.add("心超费");

            Map<String, Object> jianchaMap = new HashMap<>();
            jianchaMap.put("unit", "元");

            Map<String, Object> jianchacMap = new HashMap<>();
            jianchacMap.put("unit", "次");

            Double jianchafei = 0.0;
            int jianchacishu = 0;

            if (fenliemingxi.get(jie) != null)
                for (Map k : fenliemingxi.get(jie)) {
                    if (jiancha.contains(k.get("shoufeixiangmuleibie_5").toString())) {
                        jianchafei += Double.parseDouble(mapStringToMap(k.get("jine_20").toString()).get("value").toString());
                        jianchacishu++;
                    }
                }

            jianchaMap.put("value", jianchafei);
            map.put("jianchajianyanqiuhe_1", jianchaMap);
            jianchacMap.put("value", jianchacishu);
            map.put("jianchajianyanjici_1", jianchacMap);

            Map<String, Object> yiliaoMap = new HashMap<>();
            yiliaoMap.put("unit", "次");
            yibaoMap.put("unit", "元");

            int m = 0, z = 0;
            for (Map k : fenliejiesuan.get(jie)) {
                if (k.get("yiliaoleibie_8").equals("普通门诊")) m = 1;
                if (k.get("yiliaoleibie_8").equals("住院")) z = 1;
                if (m + z == 2) break;
            }
            yiliaoMap.put("value", m + z);
            map.put("yiliaoleibiejici_1", yiliaoMap);

            Map<String, Object> chuangweiMap = new HashMap<>();
            chuangweiMap.put("unit", "个");
            chuangweiMap.put("value", 0);
            map.put("chuangweishu_1", chuangweiMap);

            Map<String, Object> CTMap = new HashMap<>();
            CTMap.put("unit", "元");
            Map<String, Object> CTcMap = new HashMap<>();
            CTcMap.put("unit", "次");
            Double CTMoney = 0.0;
            int CTCount = 0;

            if (fenliemingxi.get(jie) != null)
                for (Map k : fenliemingxi.get(jie)) {
                    if (k.get("shoufeixiangmuleibie_5").equals("CT费")) {
                        CTMoney += Double.parseDouble(mapStringToMap(k.get("jine_20").toString()).get("value").toString());
                        CTCount++;
                    }
                }

            CTMap.put("value", CTMoney);
            map.put("CTqiuhe_1", CTMap);
            CTcMap.put("value", CTCount);
            map.put("CTjici_1", CTcMap);

            Map<String, Object> TCDMap = new HashMap<>();
            TCDMap.put("unit", "元");

            Map<String, Object> TCDcMap = new HashMap<>();
            TCDcMap.put("unit", "次");

            Double TCDMoney = 0.0;
            int TCDCount = 0;

            if (fenliemingxi.get(jie) != null)
                for (Map k : fenliemingxi.get(jie)) {
                    if (k.get("shoufeixiangmuleibie_5").equals("TCD")) {
                        TCDMoney += Double.parseDouble(mapStringToMap(k.get("jine_20").toString()).get("value").toString());
                        TCDCount++;
                    }
                }

            TCDMap.put("value", TCDMoney);
            map.put("TCDqiuhe_1", TCDMap);
            TCDcMap.put("value", TCDCount);
            map.put("TCDjici_1", TCDcMap);

            Map<String, Object> bingliMap = new HashMap<>();
            bingliMap.put("unit", "元");

            Map<String, Object> bcMap = new HashMap<>();
            bcMap.put("unit", "次");

            Double bingliMoney = 0.0;
            int bingliCount = 0;

            if (fenliemingxi.get(jie) != null)
                for (Map k : fenliemingxi.get(jie)) {
                    if (k.get("shoufeixiangmuleibie_5").equals("病理费")) {
                        bingliMoney += Double.parseDouble(mapStringToMap(k.get("jine_20").toString()).get("value").toString());
                        bingliCount++;
                    }
                }

            bingliMap.put("value", bingliMoney);
            map.put("bingliqiuhe_1", bingliMap);
            bcMap.put("value", bingliCount);
            map.put("binglijici_1", bcMap);

            Map<String, Object> cailiaoMap = new HashMap<>();
            cailiaoMap.put("unit", "元");
            Map<String, Object> caicMap = new HashMap<>();
            caicMap.put("unit", "次");

            Double cailiaoMoney = 0.0;
            int cailiaoCount = 0;

            if (fenliemingxi.get(jie) != null)
                for (Map k : fenliemingxi.get(jie)) {
                    if (k.get("shoufeixiangmuleibie_5").equals("材料费")) {
                        cailiaoMoney += Double.parseDouble(mapStringToMap(k.get("jine_20").toString()).get("value").toString());
                        cailiaoCount++;
                    }
                }

            cailiaoMap.put("value", cailiaoMoney);
            map.put("cailiaoqiuhe_1", cailiaoMap);
            caicMap.put("value", cailiaoCount);
            map.put("cailiaojici_1", caicMap);

            Map<String, Object> caichaoMap = new HashMap<>();
            caichaoMap.put("unit", "元");
            Map<String, Object> cailcMap = new HashMap<>();
            cailcMap.put("unit", "次");

            Double caichaoMoney = 0.0;
            int caichaoCount = 0;

            if (fenliemingxi.get(jie) != null)
                for (Map k : fenliemingxi.get(jie)) {
                    if (k.get("shoufeixiangmuleibie_5").equals("彩超费")) {
                        caichaoMoney += Double.parseDouble(mapStringToMap(k.get("jine_20").toString()).get("value").toString());
                        caichaoCount++;
                    }
                }

            caichaoMap.put("value", caichaoMoney);
            map.put("caichaoqiuhe_1", caichaoMap);
            cailcMap.put("value", caichaoCount);
            map.put("caichaojici_1", cailcMap);


            Map<String, Object> caoyaoMap = new HashMap<>();
            caoyaoMap.put("unit", "元");
            Map<String, Object> caocMap = new HashMap<>();
            caocMap.put("unit", "次");
            Double caoyaoMoney = 0.0;
            int caoyaoCount = 0;


            if (fenliemingxi.get(jie) != null)
                for (Map k : fenliemingxi.get(jie)) {
                    if (k.get("shoufeixiangmuleibie_5").equals("草药费")) {
                        caoyaoMoney += Double.parseDouble(mapStringToMap(k.get("jine_20").toString()).get("value").toString());
                        caoyaoCount++;
                    }
                }

            caoyaoMap.put("value", caoyaoMoney);
            map.put("caoyaoqiuhe_1", caoyaoMap);
            caocMap.put("value", caoyaoCount);
            map.put("caoyaojici_1", caocMap);


            Map<String, Object> zhongchengMap = new HashMap<>();
            zhongchengMap.put("unit", "元");
            Map<String, Object> zcMap = new HashMap<>();
            zcMap.put("unit", "次");
            Double zhongchengyaoMoney = 0.0;
            int zhongchengyaoCount = 0;


            if (fenliemingxi.get(jie) != null)
                for (Map k : fenliemingxi.get(jie)) {
                    if (k.get("shoufeixiangmuleibie_5").equals("中成药费")) {
                        zhongchengyaoMoney += Double.parseDouble(mapStringToMap(k.get("jine_20").toString()).get("value").toString());
                        zhongchengyaoCount++;
                    }
                }

            zhongchengMap.put("value", zhongchengyaoMoney);
            map.put("zhongchengyaoqiuhe_1", zhongchengMap);
            zcMap.put("value", zhongchengyaoCount);
            map.put("zhongchengyaojici_1", zcMap);


            Map<String, Object> ciMap = new HashMap<>();
            ciMap.put("unit", "元");
            Map<String, Object> cicMap = new HashMap<>();
            cicMap.put("unit", "次");
            Double cigongzhenMoney = 0.0;
            int cigongzhenCount = 0;


            if (fenliemingxi.get(jie) != null)
                for (Map k : fenliemingxi.get(jie)) {
                    if (k.get("shoufeixiangmuleibie_5").equals("磁共振")) {
                        cigongzhenMoney += Double.parseDouble(mapStringToMap(k.get("jine_20").toString()).get("value").toString());
                        cigongzhenCount++;
                    }
                }

            ciMap.put("value", cigongzhenMoney);
            map.put("cigongzhenqiuhe_1", ciMap);
            cicMap.put("value", cigongzhenCount);
            map.put("cigongzhenjici_1", cicMap);


            Map<String, Object> huMap = new HashMap<>();
            huMap.put("unit", "元");
            Map<String, Object> hucMap = new HashMap<>();
            hucMap.put("unit", "次");
            Double huliMoney = 0.0;
            int huliCount = 0;
            if (fenliemingxi.get(jie) != null)
                for (Map k : fenliemingxi.get(jie)) {
                    if (k.get("shoufeixiangmuleibie_5").equals("护理费")) {
                        huliMoney += Double.parseDouble(mapStringToMap(k.get("jine_20").toString()).get("value").toString());
                        huliCount++;
                    }
                }


            huMap.put("value", huliMoney);
            map.put("huliqiuhe_1", huMap);
            hucMap.put("value", huliCount);
            map.put("hulijici_1", hucMap);

            Map<String, Object> huaMap = new HashMap<>();
            huaMap.put("unit", "元");
            Map<String, Object> huacMap = new HashMap<>();
            huacMap.put("unit", "次");
            Double huayanMoney = 0.0;
            int huayanCount = 0;
            if (fenliemingxi.get(jie) != null)
                for (Map k : fenliemingxi.get(jie)) {
                    if (k.get("shoufeixiangmuleibie_5").equals("化验费")) {
                        huayanMoney += Double.parseDouble(mapStringToMap(k.get("jine_20").toString()).get("value").toString());
                        huayanCount++;
                    }
                }

            huaMap.put("value", huayanMoney);
            map.put("huayanqiuhe_1", huaMap);
            huacMap.put("value", huayanCount);
            map.put("huayanjici_1", huacMap);

            Map<String, Object> huanMap = new HashMap<>();
            huanMap.put("unit", "元");
            Map<String, Object> huancMap = new HashMap<>();
            huancMap.put("unit", "次");
            Double huanyaoMoney = 0.0;
            int huanyaoCount = 0;
            if (fenliemingxi.get(jie) != null)
                for (Map k : fenliemingxi.get(jie)) {
                    if (k.get("shoufeixiangmuleibie_5").equals("换药费")) {
                        huanyaoMoney += Double.parseDouble(mapStringToMap(k.get("jine_20").toString()).get("value").toString());
                        huanyaoCount++;
                    }
                }

            huanMap.put("value", huanyaoMoney);
            map.put("huanyaoqiuhe_1", huanMap);
            huancMap.put("value", huanyaoCount);
            map.put("huanyaojici_1", huancMap);

            Map<String, Object> jianMap = new HashMap<>();
            jianMap.put("unit", "元");
            Map<String, Object> jiancMap = new HashMap<>();
            jiancMap.put("unit", "次");
            Double jianchaMoney = 0.0;
            int jianchaCount = 0;
            if (fenliemingxi.get(jie) != null)
                for (Map k : fenliemingxi.get(jie)) {
                    if (k.get("shoufeixiangmuleibie_5").equals("检查费")) {
                        jianchaMoney += Double.parseDouble(mapStringToMap(k.get("jine_20").toString()).get("value").toString());
                        jianchaCount++;
                    }
                }

            jianMap.put("value", jianchaMoney);
            map.put("jianchaqiuhe_1", jianMap);
            jiancMap.put("value", jianchaCount);
            map.put("jianchajici_1", jiancMap);

            Map<String, Object> jiuMap = new HashMap<>();
            jiuMap.put("unit", "元");
            Map<String, Object> jiucMap = new HashMap<>();
            jiucMap.put("unit", "次");
            Double jiuhucheMoney = 0.0;
            int jiuhucheCount = 0;
            if (fenliemingxi.get(jie) != null)
                for (Map k : fenliemingxi.get(jie)) {
                    if (k.get("shoufeixiangmuleibie_5").equals("救护车费")) {
                        jiuhucheMoney += Double.parseDouble(mapStringToMap(k.get("jine_20").toString()).get("value").toString());
                        jiuhucheCount++;
                    }
                }

            jiuMap.put("value", jiuhucheMoney);
            map.put("jiuhucheqiuhe_1", jiuMap);
            jiucMap.put("value", jiuhucheCount);
            map.put("jiuhuchejici_1", jiucMap);

            Map<String, Object> liliaoMap = new HashMap<>();
            liliaoMap.put("unit", "元");
            Map<String, Object> liliaocMap = new HashMap<>();
            liliaocMap.put("unit", "次");
            Double liliaoMoney = 0.0;
            int liliaoCount = 0;
            if (fenliemingxi.get(jie) != null)
                for (Map k : fenliemingxi.get(jie)) {
                    if (k.get("shoufeixiangmuleibie_5").equals("理疗费")) {
                        liliaoMoney += Double.parseDouble(mapStringToMap(k.get("jine_20").toString()).get("value").toString());
                        liliaoCount++;
                    }
                }

            liliaoMap.put("value", liliaoMoney);
            map.put("liliaoqiuhe_1", liliaoMap);
            liliaocMap.put("value", liliaoCount);
            map.put("liliaojici_1", liliaocMap);

            Map<String, Object> maMap = new HashMap<>();
            maMap.put("unit", "元");
            Map<String, Object> macMap = new HashMap<>();
            macMap.put("unit", "次");
            Double mazuiMoney = 0.0;
            int mazuiCount = 0;
            if (fenliemingxi.get(jie) != null)
                for (Map k : fenliemingxi.get(jie)) {
                    if (k.get("shoufeixiangmuleibie_5").equals("麻醉费")) {
                        mazuiMoney += Double.parseDouble(mapStringToMap(k.get("jine_20").toString()).get("value").toString());
                        mazuiCount++;
                    }
                }

            maMap.put("value", mazuiMoney);
            map.put("mazuiqiuhe_1", maMap);
            macMap.put("value", mazuiCount);
            map.put("mazuijici_1", macMap);

            Map<String, Object> naoMap = new HashMap<>();
            naoMap.put("unit", "元");
            Map<String, Object> naocMap = new HashMap<>();
            naocMap.put("unit", "次");
            Double naodianMoney = 0.0;
            int naodianCount = 0;
            if (fenliemingxi.get(jie) != null)
                for (Map k : fenliemingxi.get(jie)) {
                    if (k.get("shoufeixiangmuleibie_5").equals("脑电图")) {
                        naodianMoney += Double.parseDouble(mapStringToMap(k.get("jine_20").toString()).get("value").toString());
                        naodianCount++;
                    }
                }

            naoMap.put("value", naodianMoney);
            map.put("naodiantuqiuhe_1", naoMap);
            naocMap.put("value", naodianCount);
            map.put("naodiantujici_1", naocMap);

            Map<String, Object> paiMap = new HashMap<>();
            paiMap.put("unit", "元");
            Map<String, Object> paicMap = new HashMap<>();
            paicMap.put("unit", "次");
            Double paipianMoney = 0.0;
            int paipianCount = 0;
            if (fenliemingxi.get(jie) != null)
                for (Map k : fenliemingxi.get(jie)) {
                    if (k.get("shoufeixiangmuleibie_5").equals("拍片费")) {
                        paipianMoney += Double.parseDouble(mapStringToMap(k.get("jine_20").toString()).get("value").toString());
                        paipianCount++;
                    }
                }

            paiMap.put("value", paipianMoney);
            map.put("paipianqiuhe_1", paiMap);
            paicMap.put("value", paipianCount);
            map.put("paipianjici_1", paicMap);

            Map<String, Object> ssMap = new HashMap<>();
            ssMap.put("unit", "元");
            Map<String, Object> sscMap = new HashMap<>();
            sscMap.put("unit", "次");
            Double shoushuMoney = 0.0;
            int shoushuCount = 0;
            if (fenliemingxi.get(jie) != null)
                for (Map k : fenliemingxi.get(jie)) {
                    if (k.get("shoufeixiangmuleibie_5").equals("手术材料费")) {
                        shoushuMoney += Double.parseDouble(mapStringToMap(k.get("jine_20").toString()).get("value").toString());
                        shoushuCount++;
                    }

                }

            ssMap.put("value", shoushuMoney);
            map.put("shoushucailiaoqiuhe_1", ssMap);
            sscMap.put("value", shoushuCount);
            map.put("shoushucailiaojici_1", sscMap);

            Map<String, Object> shouMap = new HashMap<>();
            shouMap.put("unit", "元");
            Map<String, Object> shoucMap = new HashMap<>();
            shoucMap.put("unit", "次");
            Double shouMoney = 0.0;
            int shouCount = 0;
            if (fenliemingxi.get(jie) != null)
                for (Map k : fenliemingxi.get(jie)) {
                    if (k.get("shoufeixiangmuleibie_5").equals("手术费")) {
                        shouMoney += Double.parseDouble(mapStringToMap(k.get("jine_20").toString()).get("value").toString());
                        shouCount++;
                    }
                }

            shouMap.put("value", shouMoney);
            map.put("shoushuqiuhe_1", shouMap);
            shoucMap.put("value", shouCount);
            map.put("shoushujici_1", shoucMap);

            Map<String, Object> tiMap = new HashMap<>();
            tiMap.put("unit", "元");
            Map<String, Object> ticMap = new HashMap<>();
            ticMap.put("unit", "次");
            Double tijianMoney = 0.0;
            int tijianCount = 0;
            if (fenliemingxi.get(jie) != null)
                for (Map k : fenliemingxi.get(jie)) {
                    if (k.get("shoufeixiangmuleibie_5").equals("体检费")) {
                        tijianMoney += Double.parseDouble(mapStringToMap(k.get("jine_20").toString()).get("value").toString());
                        tijianCount++;
                    }
                }

            tiMap.put("value", tijianMoney);
            map.put("tijianqiuhe_1", tiMap);
            ticMap.put("value", tijianCount);
            map.put("tijianjici_1", ticMap);


            Map<String, Object> weiMap = new HashMap<>();
            weiMap.put("unit", "元");
            Map<String, Object> weicMap = new HashMap<>();
            weicMap.put("unit", "次");
            Double weijingMoney = 0.0;
            int weijingCount = 0;
            if (fenliemingxi.get(jie) != null)
                for (Map k : fenliemingxi.get(jie)) {
                    if (k.get("shoufeixiangmuleibie_5").equals("胃镜费")) {
                        weijingMoney += Double.parseDouble(mapStringToMap(k.get("jine_20").toString()).get("value").toString());
                        weijingCount++;
                    }
                }

            weiMap.put("value", weijingMoney);
            map.put("weijingqiuhe_1", weiMap);
            weicMap.put("value", weijingCount);
            map.put("weijingjici_1", weicMap);

            Map<String, Object> xiMap = new HashMap<>();
            xiMap.put("unit", "元");
            Map<String, Object> xicMap = new HashMap<>();
            xicMap.put("unit", "次");
            Double xiyaoMoney = 0.0;
            int xiyaoCount = 0;
            if (fenliemingxi.get(jie) != null)
                for (Map x : fenliemingxi.get(jie)) {
                    if (x.get("shoufeixiangmuleibie_5").equals("西药费")) {
                        xiyaoMoney += Double.parseDouble(mapStringToMap(x.get("jine_20").toString()).get("value").toString());
                        xiyaoCount++;
                    }
                }
            xiMap.put("value", xiyaoMoney);
            map.put("xiyaoqiuhe_1", xiMap);
            xicMap.put("value", xiyaoCount);
            map.put("xiyaojici_1", xicMap);

            Map<String, Object> xinMap = new HashMap<>();
            xinMap.put("unit", "元");
            Map<String, Object> xincMap = new HashMap<>();
            xincMap.put("unit", "次");
            Double xinchaoMoney = 0.0;
            int xinchaoCount = 0;
            if (fenliemingxi.get(jie) != null)
                for (Map k : fenliemingxi.get(jie)) {
                    if (k.get("shoufeixiangmuleibie_5").equals("心超费")) {
                        xinchaoMoney += Double.parseDouble(mapStringToMap(k.get("jine_20").toString()).get("value").toString());
                        xinchaoCount++;
                    }
                }


            xinMap.put("value", xinchaoMoney);
            map.put("xinchaoqiuhe_1", xinMap);
            xincMap.put("value", xinchaoCount);
            map.put("xinchaojici_1", xincMap);

            Map<String, Object> zhenMap = new HashMap<>();
            zhenMap.put("unit", "元");
            Map<String, Object> zhencMap = new HashMap<>();
            zhencMap.put("unit", "次");
            Double zhenliaoMoney = 0.0;
            int zhenliaoCount = 0;
            if (fenliemingxi.get(jie) != null)
                for (Map k : fenliemingxi.get(jie)) {
                    if (k.get("shoufeixiangmuleibie_5").equals("诊疗费")) {
                        zhenliaoMoney += Double.parseDouble(mapStringToMap(k.get("jine_20").toString()).get("value").toString());
                        zhenliaoCount++;
                    }
                }


            zhenMap.put("value", zhenliaoMoney);
            map.put("zhenliaoqiuhe_1", zhenMap);
            zhencMap.put("value", zhenliaoCount);
            map.put("zhenliaojici_1", zhencMap);

            Map<String, Object> zhiMap = new HashMap<>();
            zhiMap.put("unit", "元");
            Map<String, Object> zhicMap = new HashMap<>();
            zhicMap.put("unit", "次");
            Double zhiliaoMoney = 0.0;
            int zhiliaoCount = 0;
            if (fenliemingxi.get(jie) != null)
                for (Map k : fenliemingxi.get(jie)) {
                    if (k.get("shoufeixiangmuleibie_5").equals("治疗费")) {
                        zhiliaoMoney += Double.parseDouble(mapStringToMap(k.get("jine_20").toString()).get("value").toString());
                        zhiliaoCount++;
                    }
                }


            zhiMap.put("value", zhiliaoMoney);
            map.put("zhiliaoqiuhe_1", zhiMap);
            zhicMap.put("value", zhiliaoCount);
            map.put("zhiliaojici_1", zhicMap);

            Map<String, Object> zhuMap = new HashMap<>();
            zhuMap.put("unit", "元");
            Map<String, Object> zhucMap = new HashMap<>();
            zhucMap.put("unit", "次");
            Double zhusheMoney = 0.0;
            int zhusheCount = 0;
            if (fenliemingxi.get(jie) != null)
                for (Map k : fenliemingxi.get(jie)) {
                    if (k.get("shoufeixiangmuleibie_5").equals("注射费")) {
                        zhusheMoney += Double.parseDouble(mapStringToMap(k.get("jine_20").toString()).get("value").toString());
                        zhusheCount++;
                    }
                }

            zhuMap.put("value", zhusheMoney);
            map.put("zhusheqiuhe_1", zhuMap);
            zhucMap.put("value", zhusheCount);
            map.put("zhushejici_1", zhucMap);


            Map<String, Object> zongjinerMap = new HashMap<>();
            zongjinerMap.put("unit", "元");
            Double zongjiner = 0.0;
            for (Map k : fenliejiesuan.get(jie)) {
                zongjiner += Double.parseDouble(mapStringToMap(k.get("yiliaofeizonge_6").toString()).get("value").toString());
            }

            zongjinerMap.put("value", zongjiner);
            map.put("danweishijianzongjine_1", zongjinerMap);

            Map<String, Object> yibaozongMap = new HashMap<>();
            yibaozongMap.put("unit", "元");
            Double yibaozong = 0.0;
            for (Map k : fenliejiesuan.get(jie)) {
                yibaozong += Double.parseDouble(mapStringToMap(k.get("tongchouzhifujine_5").toString()).get("value").toString());
            }

            yibaozongMap.put("value", yibaozong);
            map.put("danweishijianyibaobaoxiaozonge_1", yibaozongMap);

            Map<String, Object> biliMap = new HashMap<>();
            biliMap.put("unit", "比率");
            Double bili = yibaozong / zongjiner;
            biliMap.put("value", bili);
            map.put("danweishijianyibaobili_1", biliMap);

            Map<String, Object> keshiMap = new HashMap<>();
            keshiMap.put("unit", "个");
            Map<Object, Object> quchong = new HashMap<>();

            if (fenliemingxi.get(jie) != null)
                for (Map k : fenliemingxi.get(jie)) {
                    quchong.put(k.get("keshimingcheng_26"), "1");
                }

            keshiMap.put("value", quchong.size());
            map.put("danweishijiankeshishuliang_1", keshiMap);

            Map<String, Object> yishengshuliangMap = new HashMap<>();
            yishengshuliangMap.put("unit", "个");
            yishengshuliangMap.put("value", 1);

            map.put("danweishijianyishengshuliang_1", yishengshuliangMap);

            Map<String, Object> chuangMap = new HashMap<>();
            chuangMap.put("unit", "个");
            chuangMap.put("value", 0);
            map.put("danweishijianchuangweishu_1", chuangMap);

            Map<String, Object> bbiliMap = new HashMap<>();
            bbiliMap.put("unit", "比率");
            bbiliMap.put("value", 0);
            map.put("danweishijianbendiyidibili_1", bbiliMap);

            Map<String, Object> jiezhenMap = new HashMap<>();
            jiezhenMap.put("unit", "个");
            Map<Object, Object> jiezhen = new HashMap<>();
            for (Map k : fenliejiesuan.get(jie)) {
                jiezhen.put(k.get("xingming_103"), "1");
            }

            jiezhenMap.put("value", jiezhen.size());
            map.put("danweishijianneijiezhenrenci_1", jiezhenMap);

            Map<String, Object> zhuyuanMap = new HashMap<>();
            zhuyuanMap.put("unit", "个");
            Map<Object, Object> zhuyuan = new HashMap<>();
            for (Map k : fenliejiesuan.get(jie)) {
                if (k.get("yiliaoleibie_8").equals("住院")) {
                    zhuyuan.put(k.get("xingming_103"), "1");
                }
            }

            zhuyuanMap.put("value", zhuyuan.size());
            map.put("danweishijianzhuyuanrenci_1", zhuyuanMap);

            Map<String, Object> shoushulMap = new HashMap<>();
            shoushulMap.put("unit", "个");
            Map<Object, Object> shoushu = new HashMap<>();
            if (fenliemingxi.get(jie) != null)
                for (Map k : fenliemingxi.get(jie)) {
                    if (k.get("shoufeixiangmuleibie_5").toString().contains("手术")) {
                        shoushu.put(k.get("xingming_103"), "1");
                    }
                }

            shoushulMap.put("value", shoushu.size());
            map.put("danweishijianshoushurenci_1", shoushulMap);

            map.put("danweishijiankoufuyaozhonglei_1", "0");

            if (fenliemingxi.get(jie) != null)
                fenliemingxi.get(jie).clear();
            fenliejiesuan.get(jie).clear();
            mongoTemplate.insert(map, yishengbiaoku);
            // map = new HashMap<>();
            //  System.out.println(jie);
        }
        System.out.println(mongoTemplate.count(new Query(), yishengbiaoku));
        //  break;
    }

    @Test
    void yishengbiaodaoru2(int limit) {
        List<Map> mingxiyisheng = new ArrayList<>();
        Map<String, List<Map>> fenliemingxi = new HashMap<>();
        String yishengbiaoku = "com.ns.entity.object.form.instance.nsjgpv79iu55j528ig78.yishengbiao_1";
        String jiesuanku = "com.ns.entity.object.form.instance.nsjgpv79iu55j528ig78.feiyongjiesuanxinxi_1";
        String mingxiku = "com.ns.entity.object.form.instance.nsjgpv79iu55j528ig78.feiyongmingxixinxi_1";

        Query q1 = new Query();
        q1.fields().include("zhuzhiyishengdaima_1").exclude("_id");
        List<Map> one = new ArrayList<>();
        one = mongoTemplate.find(q1, Map.class, jiesuanku);
        Set<Map> yibianma = new HashSet<>(one);

        Query q3 = new Query();
        q3.fields().include("jine_20").include("shoufeixiangmuleibie_5").include("xingming_103")
                .include("yishengbianma_5").exclude("_id");


        for (int s = limit; s <= limit + 499; s++) {
            mingxiyisheng = mongoTemplate.find(q3.skip(s * 3250).limit(3250), Map.class, mingxiku);
            if (mingxiyisheng.size() == 0) {
                break;
            }
            for (Map k : mingxiyisheng) {
                String fieldValue = k.get("yishengbianma_5").toString(); // 假设用于分组的字段名为 "fieldName"
                if (fenliemingxi.get(fieldValue) == null) {
                    fenliemingxi.put(fieldValue, new ArrayList<>());
                }
                fenliemingxi.get(fieldValue).add(k);
            }
            System.out.println(s);
            mingxiyisheng.clear();
        }

        for (Map mapstrjie : yibianma) {
            String jie = mapstrjie.get("zhuzhiyishengdaima_1").toString();
            Update map = new Update();

            Query q4 = new Query(Criteria.where("yishengbianma_9").is(jie));
            Map<String, Object> yuan = mongoTemplate.findOne(q4, Map.class, yishengbiaoku);

            if (yuan.isEmpty()) {
                break;
            }

            ArrayList<String> jiancha = new ArrayList<>();
            jiancha.add("CT费");
            jiancha.add("TCD");
            jiancha.add("彩超费");
            jiancha.add("磁共振");
            jiancha.add("化验费");
            jiancha.add("检查费");
            jiancha.add("脑电图");
            jiancha.add("拍片费");
            jiancha.add("胃镜费");
            jiancha.add("心超费");

            Map<String, Object> jianchaMap = new HashMap<>();
            jianchaMap.put("unit", "元");

            Map<String, Object> jianchacMap = new HashMap<>();
            jianchacMap.put("unit", "次");

            Object yjc = yuan.get("jianchajianyanqiuhe_1");
            Object yjcc = yuan.get("jianchajianyanjici_1");

            Double jianchafei = Double.parseDouble(mapStringToMap(yjc.toString()).get("value").toString());
            ;
            int jianchacishu = Integer.parseInt(mapStringToMap(yjcc.toString()).get("value").toString());
            ;
            if (fenliemingxi.get(jie) != null)
                for (Map k : fenliemingxi.get(jie)) {
                    if (jiancha.contains(k.get("shoufeixiangmuleibie_5").toString())) {
                        jianchafei += Double.parseDouble(mapStringToMap(k.get("jine_20").toString()).get("value").toString());
                        jianchacishu++;
                    }
                }

            jianchaMap.put("value", jianchafei);
            map.set("jianchajianyanqiuhe_1", jianchaMap);
            jianchacMap.put("value", jianchacishu);
            map.set("jianchajianyanjici_1", jianchacMap);

            Object yct = yuan.get("CTqiuhe_1");
            Object yctc = yuan.get("CTjici_1");

            Double CTMoney = Double.parseDouble(mapStringToMap(yct.toString()).get("value").toString());
            ;
            int CTCount = Integer.parseInt(mapStringToMap(yctc.toString()).get("value").toString());
            ;

            Map<String, Object> CTMap = new HashMap<>();
            CTMap.put("unit", "元");
            Map<String, Object> CTcMap = new HashMap<>();
            CTcMap.put("unit", "次");
            if (fenliemingxi.get(jie) != null)
                for (Map k : fenliemingxi.get(jie)) {
                    if (k.get("shoufeixiangmuleibie_5").equals("CT费")) {
                        CTMoney += Double.parseDouble(mapStringToMap(k.get("jine_20").toString()).get("value").toString());
                        CTCount++;
                    }
                }
            CTMap.put("value", CTMoney);
            map.set("CTqiuhe_1", CTMap);
            CTcMap.put("value", CTCount);
            map.set("CTjici_1", CTcMap);

            Object ytcd = yuan.get("TCDqiuhe_1");
            Object ytcdc = yuan.get("TCDjici_1");

            Double TCDMoney = Double.parseDouble(mapStringToMap(ytcd.toString()).get("value").toString());
            ;
            int TCDCount = Integer.parseInt(mapStringToMap(ytcdc.toString()).get("value").toString());
            ;

            Map<String, Object> TCDMap = new HashMap<>();
            TCDMap.put("unit", "元");

            Map<String, Object> TCDcMap = new HashMap<>();
            TCDcMap.put("unit", "次");

            if (fenliemingxi.get(jie) != null)
                for (Map k : fenliemingxi.get(jie)) {
                    if (k.get("shoufeixiangmuleibie_5").equals("TCD")) {
                        TCDMoney += Double.parseDouble(mapStringToMap(k.get("jine_20").toString()).get("value").toString());
                        TCDCount++;
                    }
                }

            TCDMap.put("value", TCDMoney);
            map.set("TCDqiuhe_1", TCDMap);
            TCDcMap.put("value", TCDCount);
            map.set("TCDjici_1", TCDcMap);

            Object ybl = yuan.get("bingliqiuhe_1");
            Object yblc = yuan.get("binglijici_1");

            Double bingliMoney = Double.parseDouble(mapStringToMap(ybl.toString()).get("value").toString());
            ;
            int bingliCount = Integer.parseInt(mapStringToMap(yblc.toString()).get("value").toString());
            ;

            Map<String, Object> bingliMap = new HashMap<>();
            bingliMap.put("unit", "元");

            Map<String, Object> bcMap = new HashMap<>();
            bcMap.put("unit", "次");
            if (fenliemingxi.get(jie) != null)
                for (Map k : fenliemingxi.get(jie)) {
                    if (k.get("shoufeixiangmuleibie_5").equals("病理费")) {
                        bingliMoney += Double.parseDouble(mapStringToMap(k.get("jine_20").toString()).get("value").toString());
                        bingliCount++;
                    }
                }

            bingliMap.put("value", bingliMoney);
            map.set("bingliqiuhe_1", bingliMap);
            bcMap.put("value", bingliCount);
            map.set("binglijici_1", bcMap);

            Map<String, Object> cailiaoMap = new HashMap<>();
            cailiaoMap.put("unit", "元");
            Map<String, Object> caicMap = new HashMap<>();
            caicMap.put("unit", "次");

            Object ycl = yuan.get("cailiaoqiuhe_1");
            Object yclc = yuan.get("cailiaojici_1");

            Double cailiaoMoney = Double.parseDouble(mapStringToMap(ycl.toString()).get("value").toString());
            ;
            int cailiaoCount = Integer.parseInt(mapStringToMap(yclc.toString()).get("value").toString());
            ;


            if (fenliemingxi.get(jie) != null)
                for (Map k : fenliemingxi.get(jie)) {
                    if (k.get("shoufeixiangmuleibie_5").equals("材料费")) {
                        cailiaoMoney += Double.parseDouble(mapStringToMap(k.get("jine_20").toString()).get("value").toString());
                        cailiaoCount++;
                    }
                }

            cailiaoMap.put("value", cailiaoMoney);
            map.set("cailiaoqiuhe_1", cailiaoMap);
            caicMap.put("value", cailiaoCount);
            map.set("cailiaojici_1", caicMap);


            Map<String, Object> caichaoMap = new HashMap<>();
            caichaoMap.put("unit", "元");
            Map<String, Object> caichcMap = new HashMap<>();
            caichcMap.put("unit", "次");

            Object ycc = yuan.get("caichaoqiuhe_1");
            Object yccc = yuan.get("caichaojici_1");

            Double caichaoMoney = Double.parseDouble(mapStringToMap(ycc.toString()).get("value").toString());
            int caichaoCount = Integer.parseInt(mapStringToMap(yccc.toString()).get("value").toString());

            if (fenliemingxi.get(jie) != null)
                for (Map k : fenliemingxi.get(jie)) {
                    if (k.get("shoufeixiangmuleibie_5").equals("彩超费")) {
                        caichaoMoney += Double.parseDouble(mapStringToMap(k.get("jine_20").toString()).get("value").toString());
                        caichaoCount++;
                    }
                }

            caichaoMap.put("value", caichaoMoney);
            map.set("caichaoqiuhe_1", caichaoMap);
            caichcMap.put("value", caichaoCount);
            map.set("caichaojici_1 ", caichcMap);


            Map<String, Object> caoyaoMap = new HashMap<>();
            caoyaoMap.put("unit", "元");
            Map<String, Object> caocMap = new HashMap<>();
            caocMap.put("unit", "次");

            Object ycy = yuan.get("caoyaoqiuhe_1");
            Object ycyc = yuan.get("caoyaojici_1");

            Double caoyaoMoney = Double.parseDouble(mapStringToMap(ycy.toString()).get("value").toString());
            ;
            int caoyaoCount = Integer.parseInt(mapStringToMap(ycyc.toString()).get("value").toString());
            ;


            if (fenliemingxi.get(jie) != null)
                for (Map k : fenliemingxi.get(jie)) {
                    if (k.get("shoufeixiangmuleibie_5").equals("草药费")) {
                        caoyaoMoney += Double.parseDouble(mapStringToMap(k.get("jine_20").toString()).get("value").toString());
                        caoyaoCount++;
                    }
                }

            caoyaoMap.put("value", caoyaoMoney);
            map.set("caoyaoqiuhe_1", caoyaoMap);
            caocMap.put("value", caoyaoCount);
            map.set("caoyaojici_1", caocMap);


            Map<String, Object> zhongchengMap = new HashMap<>();
            zhongchengMap.put("unit", "元");
            Map<String, Object> zcMap = new HashMap<>();
            zcMap.put("unit", "次");

            Object yzcy = yuan.get("zhongchengyaoqiuhe_1");
            Object yzcyc = yuan.get("zhongchengyaojici_1");

            Double zhongchengyaoMoney = Double.parseDouble(mapStringToMap(yzcy.toString()).get("value").toString());
            ;
            int zhongchengyaoCount = Integer.parseInt(mapStringToMap(yzcyc.toString()).get("value").toString());
            ;


            if (fenliemingxi.get(jie) != null)
                for (Map k : fenliemingxi.get(jie)) {
                    if (k.get("shoufeixiangmuleibie_5").equals("中成药费")) {
                        zhongchengyaoMoney += Double.parseDouble(mapStringToMap(k.get("jine_20").toString()).get("value").toString());
                        zhongchengyaoCount++;
                    }
                }

            zhongchengMap.put("value", zhongchengyaoMoney);
            map.set("zhongchengyaoqiuhe_1", zhongchengMap);
            zcMap.put("value", zhongchengyaoCount);
            map.set("zhongchengyaojici_1", zcMap);

            Map<String, Object> ciMap = new HashMap<>();
            ciMap.put("unit", "元");
            Map<String, Object> cicMap = new HashMap<>();
            cicMap.put("unit", "次");

            Object ycgz = yuan.get("cigongzhenqiuhe_1");
            Object ycgzc = yuan.get("cigongzhenjici_1");

            Double cigongzhenMoney = Double.parseDouble(mapStringToMap(ycgz.toString()).get("value").toString());
            ;
            int cigongzhenCount = Integer.parseInt(mapStringToMap(ycgzc.toString()).get("value").toString());
            ;

            if (fenliemingxi.get(jie) != null)
                for (Map k : fenliemingxi.get(jie)) {
                    if (k.get("shoufeixiangmuleibie_5").equals("磁共振")) {
                        cigongzhenMoney += Double.parseDouble(mapStringToMap(k.get("jine_20").toString()).get("value").toString());
                        cigongzhenCount++;
                    }
                }

            ciMap.put("value", cigongzhenMoney);
            map.set("cigongzhenqiuhe_1", ciMap);
            cicMap.put("value", cigongzhenCount);
            map.set("cigongzhenjici_1", cicMap);


            Map<String, Object> huMap = new HashMap<>();
            huMap.put("unit", "元");
            Map<String, Object> hucMap = new HashMap<>();
            hucMap.put("unit", "次");

            Object yhl = yuan.get("huliqiuhe_1");
            Object yhlc = yuan.get("hulijici_1");

            Double huliMoney = Double.parseDouble(mapStringToMap(yhl.toString()).get("value").toString());
            ;
            int huliCount = Integer.parseInt(mapStringToMap(yhlc.toString()).get("value").toString());
            ;

            if (fenliemingxi.get(jie) != null)
                for (Map k : fenliemingxi.get(jie)) {
                    if (k.get("shoufeixiangmuleibie_5").equals("护理费")) {
                        huliMoney += Double.parseDouble(mapStringToMap(k.get("jine_20").toString()).get("value").toString());
                        huliCount++;
                    }
                }


            huMap.put("value", huliMoney);
            map.set("huliqiuhe_1", huMap);
            hucMap.put("value", huliCount);
            map.set("hulijici_1", hucMap);

            Map<String, Object> huaMap = new HashMap<>();
            huaMap.put("unit", "元");
            Map<String, Object> huacMap = new HashMap<>();
            huacMap.put("unit", "次");

            Object yhy = yuan.get("huayanqiuhe_1");
            Object yhyc = yuan.get("huayanjici_1");

            Double huayanMoney = Double.parseDouble(mapStringToMap(yhy.toString()).get("value").toString());
            ;
            int huayanCount = Integer.parseInt(mapStringToMap(yhyc.toString()).get("value").toString());
            ;

            if (fenliemingxi.get(jie) != null)
                for (Map k : fenliemingxi.get(jie)) {
                    if (k.get("shoufeixiangmuleibie_5").equals("化验费")) {
                        huayanMoney += Double.parseDouble(mapStringToMap(k.get("jine_20").toString()).get("value").toString());
                        huayanCount++;
                    }
                }

            huaMap.put("value", huayanMoney);
            map.set("huayanqiuhe_1", huaMap);
            huacMap.put("value", huayanCount);
            map.set("huayanjici_1", huacMap);

            Map<String, Object> huanMap = new HashMap<>();
            huanMap.put("unit", "元");
            Map<String, Object> huancMap = new HashMap<>();
            huancMap.put("unit", "次");

            Object yhyf = yuan.get("huayanqiuhe_1");
            Object yhyfc = yuan.get("huayanjici_1");

            Double huanyaoMoney = Double.parseDouble(mapStringToMap(yhyf.toString()).get("value").toString());
            ;
            int huanyaoCount = Integer.parseInt(mapStringToMap(yhyfc.toString()).get("value").toString());
            ;
            if (fenliemingxi.get(jie) != null)
                for (Map k : fenliemingxi.get(jie)) {
                    if (k.get("shoufeixiangmuleibie_5").equals("换药费")) {
                        huanyaoMoney += Double.parseDouble(mapStringToMap(k.get("jine_20").toString()).get("value").toString());
                        huanyaoCount++;
                    }
                }

            huanMap.put("value", huanyaoMoney);
            map.set("huanyaoqiuhe_1", huanMap);
            huancMap.put("value", huanyaoCount);
            map.set("huanyaojici_1", huancMap);


            Map<String, Object> jianMap = new HashMap<>();
            jianMap.put("unit", "元");
            Map<String, Object> jiancMap = new HashMap<>();
            jiancMap.put("unit", "次");

            Object yjcf = yuan.get("jianchaqiuhe_1");
            Object yjcfc = yuan.get("jianchajici_1");

            Double jianchaMoney = Double.parseDouble(mapStringToMap(yjcf.toString()).get("value").toString());
            ;
            int jianchaCount = Integer.parseInt(mapStringToMap(yjcfc.toString()).get("value").toString());
            ;
            if (fenliemingxi.get(jie) != null)
                for (Map k : fenliemingxi.get(jie)) {
                    if (k.get("shoufeixiangmuleibie_5").equals("检查费")) {
                        jianchaMoney += Double.parseDouble(mapStringToMap(k.get("jine_20").toString()).get("value").toString());
                        jianchaCount++;
                    }
                }

            jianMap.put("value", jianchaMoney);
            map.set("jianchaqiuhe_1", jianMap);
            jiancMap.put("value", jianchaCount);
            map.set("jianchajici_1", jiancMap);

            Map<String, Object> xinMap = new HashMap<>();
            xinMap.put("unit", "元");
            Map<String, Object> xincMap = new HashMap<>();
            xincMap.put("unit", "次");

            Object yxc = yuan.get("xinchaoqiuhe_1");
            Object yxcc = yuan.get("xinchaojici_1");

            Double xinchaoMoney = Double.parseDouble(mapStringToMap(yxc.toString()).get("value").toString());
            ;
            int xinchaoCount = Integer.parseInt(mapStringToMap(yxcc.toString()).get("value").toString());
            ;
            if (fenliemingxi.get(jie) != null)
                for (Map k : fenliemingxi.get(jie)) {
                    if (k.get("shoufeixiangmuleibie_5").equals("心超费")) {
                        xinchaoMoney += Double.parseDouble(mapStringToMap(k.get("jine_20").toString()).get("value").toString());
                        xinchaoCount++;
                    }
                }

            xinMap.put("value", xinchaoMoney);
            map.set("xinchaoqiuhe_1", xinMap);
            xincMap.put("value", xinchaoCount);
            map.set("xinchaojici_1", xincMap);

            Map<String, Object> zhenMap = new HashMap<>();
            zhenMap.put("unit", "元");
            Map<String, Object> zhencMap = new HashMap<>();
            zhencMap.put("unit", "次");
            Object yzl = yuan.get("zhenliaoqiuhe_1");
            Object yzlc = yuan.get("zhenliaojici_1");

            Double zhenliaoMoney = Double.parseDouble(mapStringToMap(yzl.toString()).get("value").toString());
            ;
            int zhenliaoCount = Integer.parseInt(mapStringToMap(yzlc.toString()).get("value").toString());
            ;

            if (fenliemingxi.get(jie) != null)
                for (Map k : fenliemingxi.get(jie)) {
                    if (k.get("shoufeixiangmuleibie_5").equals("诊疗费")) {
                        zhenliaoMoney += Double.parseDouble(mapStringToMap(k.get("jine_20").toString()).get("value").toString());
                        zhenliaoCount++;
                    }
                }


            zhenMap.put("value", zhenliaoMoney);
            map.set("zhenliaoqiuhe_1", zhenMap);
            zhencMap.put("value", zhenliaoCount);
            map.set("zhenliaojici_1", zhencMap);


            Map<String, Object> zhiMap = new HashMap<>();
            zhiMap.put("unit", "元");
            Map<String, Object> zhicMap = new HashMap<>();
            zhicMap.put("unit", "次");

            Object yzlf = yuan.get("zhiliaoqiuhe_1");
            Object yzlfc = yuan.get("zhiliaojici_1");

            Double zhiliaoMoney = Double.parseDouble(mapStringToMap(yzlf.toString()).get("value").toString());
            ;
            int zhiliaoCount = Integer.parseInt(mapStringToMap(yzlfc.toString()).get("value").toString());
            ;
            if (fenliemingxi.get(jie) != null)
                for (Map k : fenliemingxi.get(jie)) {
                    if (k.get("shoufeixiangmuleibie_5").equals("治疗费")) {
                        zhiliaoMoney += Double.parseDouble(mapStringToMap(k.get("jine_20").toString()).get("value").toString());
                        zhiliaoCount++;
                    }
                }

            zhiMap.put("value", zhiliaoMoney);
            map.set("zhiliaoqiuhe_1", zhiMap);
            zhicMap.put("value", zhiliaoCount);
            map.set("zhiliaojici_1", zhicMap);

            Map<String, Object> zhuMap = new HashMap<>();
            zhuMap.put("unit", "元");
            Map<String, Object> zhucMap = new HashMap<>();
            zhucMap.put("unit", "次");

            Object yzs = yuan.get("zhusheqiuhe_1");
            Object yzsc = yuan.get("zhushejici_1");

            Double zhusheMoney = Double.parseDouble(mapStringToMap(yzs.toString()).get("value").toString());
            ;
            int zhusheCount = Integer.parseInt(mapStringToMap(yzsc.toString()).get("value").toString());
            ;
            if (fenliemingxi.get(jie) != null)
                for (Map k : fenliemingxi.get(jie)) {
                    if (k.get("shoufeixiangmuleibie_5").equals("注射费")) {
                        zhusheMoney += Double.parseDouble(mapStringToMap(k.get("jine_20").toString()).get("value").toString());
                        zhusheCount++;
                    }
                }

            zhuMap.put("value", zhusheMoney);
            map.set("zhusheqiuhe_1", zhuMap);
            zhucMap.put("value", zhusheCount);
            map.set("zhushejici_1", zhucMap);

            Map<String, Object> keshiMap = new HashMap<>();
            keshiMap.put("unit", "个");
            Map<Object, Object> quchong = new HashMap<>();

            Object yqz = yuan.get("danweishijiankeshishuliang_1");
            int qz = Integer.parseInt(mapStringToMap(yqz.toString()).get("value").toString());
            ;
            if (fenliemingxi.get(jie) != null)
                for (Map k : fenliemingxi.get(jie)) {
                    quchong.put(k.get("keshimingcheng_26"), "1");
                }

            keshiMap.put("value", qz + quchong.size());
            map.set("danweishijiankeshishuliang_1", keshiMap);

            Map<String, Object> shoushulMap = new HashMap<>();
            shoushulMap.put("unit", "个");
            Map<Object, Object> shoushu = new HashMap<>();

            Object yss = yuan.get("danweishijiankeshishuliang_1");
            int ss = Integer.parseInt(mapStringToMap(yss.toString()).get("value").toString());
            ;
            if (fenliemingxi.get(jie) != null)
                for (Map k : fenliemingxi.get(jie)) {
                    if (k.get("shoufeixiangmuleibie_5").toString().contains("手术")) {
                        shoushu.put(k.get("xingming_103"), "1");
                    }
                }

            shoushulMap.put("value", ss + shoushu.size());
            map.set("danweishijianshoushurenci_1", shoushulMap);

            mongoTemplate.upsert(q4, map, yishengbiaoku);
        }
        System.out.println(mongoTemplate.count(new Query(), yishengbiaoku));
    }


    @Test
    void huanzhebiaodaoru() {
        String huanzhebiaoku = "com.ns.entity.object.form.instance.nsjgpv79iu55j528ig78.huanzhebiao_1";
        String jiesuanku = "com.ns.entity.object.form.instance.nsjgpv79iu55j528ig78.feiyongjiesuanxinxi_1";
        String mingxiku = "com.ns.entity.object.form.instance.nsjgpv79iu55j528ig78.feiyongmingxixinxi_1";
        int iii = 0;

        TypedAggregation<Map> TypedAggregation3 = Aggregation.newAggregation(
                Map.class,
                Aggregation.group("xingming_103"), Aggregation.limit(100));

        AggregationResults<Map> aggregationResults3 = mongoTemplate.aggregate(
                TypedAggregation3,
                jiesuanku,
                Map.class);
        List<Map> mappedResults3 = aggregationResults3.getMappedResults();
        for (Map map : mappedResults3) {
            System.out.println(map);
        }
        TypedAggregation<Map> TypedAggregation = Aggregation.newAggregation(
                Map.class,
                Aggregation.group("xingming_104", "shoufeixiangmuleibie_5")
                        .count().as("count")
                        .sum("jine_20.value").as("value"));
        AggregationResults<Map> aggregationResults = mongoTemplate.aggregate(
                TypedAggregation,
                mingxiku,
                Map.class);


        List<Map> mappedResults = aggregationResults.getMappedResults();
        Map<String, Map<String, Map>> m1 = new HashMap<>();
        for (Map<String, Map> k : mappedResults) {
            m1.put(k.get("_id").get("xingming_104").toString()
                    + k.get("_id").get("shoufeixiangmuleibie_5").toString(), k);
        }
/*
        TypedAggregation<Map> TypedAggregation3 = Aggregation.newAggregation(
                Map.class,
                Aggregation.group("xingming_103"));

        AggregationResults<Map> aggregationResults3 = mongoTemplate.aggregate(
                TypedAggregation3,
                jiesuanku,
                Map.class);

        List<Map> mappedResults3 = aggregationResults3.getMappedResults();*/

        Map<String, Map> m3 = new HashMap<>();

        for (Map k : mappedResults3) {
            m3.put((String) k.get("_id"), k);
        }


        TypedAggregation<Map> TypedAggregation2 = Aggregation.newAggregation(
                Map.class,
                Aggregation.group("xingming_103")
                        .count().as("count"));
        AggregationResults<Map> aggregationResults2 = mongoTemplate.aggregate(
                TypedAggregation2,
                jiesuanku,
                Map.class);

        List<Map> mappedResults2 = aggregationResults2.getMappedResults();
        Map<String, Map> m2 = new HashMap<>();

        for (Map k : mappedResults2) {
            m2.put((String) k.get("_id"), k);
        }

        Query q1 = new Query();
        q1.fields().include("nianling_23").include("xingming_103")
                .include("xingbie_22").include("chushengriqi_28")
                .include("danweimingcheng_25").include("yiliaoleibie_8")
                .include("ruyuankeshimingcheng_2").include("chuyuanjibingmingcheng_3")
                .include("ruyuanjibingmingcheng_3")
                .include("yiliaofeizonge_6").include("tongchouzhifujine_5")
                .include("huzhiyisheng_1").include("chuyuankeshimingcheng_2")
                .include("chuyuanjibingzhenduanbianma_3").include("ruyuanjibingzhenduanbianma_3")
                .include("chuyuanriqi_2").include("ruyuanriqi_2")
                .include("danweimingcheng_25").exclude("_id");


        List<Map> jiehuanzhe = mongoTemplate.find(q1, Map.class, jiesuanku);

        Map<String, List<Map>> fenliejiesuan = new HashMap<>();
        for (Map k : jiehuanzhe) {
            String fieldValue = k.get("xingming_103").toString(); // 假设用于分组的字段名为 "fieldName"
            if (fenliejiesuan.get(fieldValue) == null) {
                fenliejiesuan.put(fieldValue, new ArrayList<>());
            }
            fenliejiesuan.get(fieldValue).add(k);
        }

        jiehuanzhe.clear();
        //mappedResults.clear();
        // mappedResults3.clear();
        List<Map> insertList = new ArrayList<>();
        for (int ii = 0; ii < mappedResults2.size(); ii++) {
            String name = mappedResults2.get(ii).get("_id").toString();
            if (fenliejiesuan.get(name) == null) {
                continue;
            }

            Map<String, Object> map = new HashMap<>();
            //系统字段
            String mainId = UUID.randomUUID().toString();
            map.put("_id", mainId);
            map.put("create_time", System.currentTimeMillis());
            map.put("create_account", "admin");
            map.put("category_id", "9e73ddbb-da60-48fe-8976-a6216685e166");
            map.put("data_status", "已归档");
            map.put("data_type", 1);
            map.put("priority", "");
            map.put("bind_id", mainId);
            map.put("corp_id", "nsrcu88p7uy22m7i9ioz");
            map.put("parent_corp_id_list", new ArrayList<>());
            map.put("bind_category_id", "");


            //业务字段
            //证件号码
            map.put("zhengjianhaoma_28", "");
            //社保卡号
            map.put("shehuibaozhangkahao_11", "");
            //年龄
            if (fenliejiesuan.get(name).get(0) != null) {
                if (fenliejiesuan.get(name).get(0).get("nianling_23") != null)
                    map.put("nianling_27", fenliejiesuan.get(name).get(0).get("nianling_23"));
                //姓名
                if (fenliejiesuan.get(name).get(0).get("xingming_103") != null)
                    map.put("xingming_109", fenliejiesuan.get(name).get(0).get("xingming_103"));
                //性别
                if (fenliejiesuan.get(name).get(0).get("xingbie_22") != null)
                    map.put("xingbie_26", fenliejiesuan.get(name).get(0).get("xingbie_22"));
                //出生日期
                if (fenliejiesuan.get(name).get(0).get("chushengriqi_28") != null)
                    map.put("chushengriqi_32", fenliejiesuan.get(name).get(0).get("chushengriqi_28"));

                //患者工作单位
                if (fenliejiesuan.get(name).get(0).get("danweimingcheng_25") != null)
                    map.put("huanzhegongzuodanwei_1", fenliejiesuan.get(name).get(0).get("danweimingcheng_25"));
                //医疗类别
                if (fenliejiesuan.get(name).get(0).get("yiliaoleibie_8") != null)
                    map.put("yiliaoleibie_16", fenliejiesuan.get(name).get(0).get("yiliaoleibie_8"));
                //科室名称
                if (fenliejiesuan.get(name).get(0).get("ruyuankeshimingcheng_2") != null)
                    map.put("keshimingcheng_34", fenliejiesuan.get(name).get(0).get("ruyuankeshimingcheng_2"));
                //执行科室名称
                if (fenliejiesuan.get(name).get(0).get("ruyuankeshimingcheng_2") != null)
                    map.put("zhixingkeshimingcheng_14", fenliejiesuan.get(name).get(0).get("ruyuankeshimingcheng_2"));
                //主诊疾病名称
                if (fenliejiesuan.get(name).get(0).get("chuyuanjibingmingcheng_3") != null)
                    map.put("zhuzhenjibingmingcheng_7", fenliejiesuan.get(name).get(0).get("chuyuanjibingmingcheng_3"));
                else if (fenliejiesuan.get(name).get(0).get("ruyuanjibingmingcheng_3") != null)
                    map.put("zhuzhenjibingmingcheng_7", fenliejiesuan.get(name).get(0).get("ruyuanjibingmingcheng_3"));
                //医生姓名
                if (fenliejiesuan.get(name).get(0).get("zhuzhiyisheng_1") != null)
                    map.put("yishengxingming_7", fenliejiesuan.get(name).get(0).get("zhuzhiyisheng_1"));
            }

            //统筹区
            map.put("canbaorentongchouqumingcheng_9", "");
            //医保年度
            map.put("yibaoniandu_12", 2023);
            //险种类型
            map.put("xianzhongleixing_8", "医保");

            map.put("yiyuanmingcheng_12", "孝感市第一人民医院");
            map.put("yiyuanleibie_10", "公立医院");
            map.put("yiyuandengji_10", "三级");
            map.put("yiyuanxingzhi_10", "");

            //总金额
            Map<String, Object> zje = new HashMap<>();
            zje.put("unit", "元");

            Double zongjine = 0.0;
            if (fenliejiesuan.get(name) != null)
                for (int i = 0; i < fenliejiesuan.get(name).size(); i++) {
                    zongjine += Double.parseDouble(mapStringToMap(fenliejiesuan.get(name).get(i).get("yiliaofeizonge_6").toString()).get("value").toString());
                }
            zje.put("value", zongjine);
            map.put("zongjine_11", zje);
            //医保范围费用

            Map<String, Object> yzje = new HashMap<>();
            yzje.put("unit", "元");
            Double ybzongjine = 0.0;
            if (fenliejiesuan.get(name) != null)
                for (int i = 0; i < fenliejiesuan.get(name).size(); i++) {
                    ybzongjine += Double.parseDouble(mapStringToMap(fenliejiesuan.get(name).get(i).get("tongchouzhifujine_5").toString()).get("value").toString());
                }
            yzje.put("value", ybzongjine);
            map.put("yibaofanweifeiyong_11", yzje);
            //是否住院
            if (map.get("yiliaoleibie_16") != null) {
                String yesorno = map.get("yiliaoleibie_16").equals("住院") ? "是" : "否";
                map.put("shifouzhuyuan_1", yesorno);
            }

            //医生编码计次
            Map<String, Object> bmcishu = new HashMap<>();
            bmcishu.put("unit", "次");
            bmcishu.put("value", 1);
            map.put("yishengbianmajicishu_1", bmcishu);


            Map<String, Object> chuangweiMap = new HashMap<>();
            chuangweiMap.put("unit", "个");
            chuangweiMap.put("value", m3.get(name).get("_id"));
            map.put("chuangweishu_1", chuangweiMap);
            //CT求和
            Map<String, Object> CTMap = new HashMap<>();
            CTMap.put("unit", "元");
            CTMap.put("value", 0);
            Map<String, Object> CTcMap = new HashMap<>();
            CTcMap.put("unit", "次");
            CTcMap.put("value", 0);

            if (m1.get(name + "CT费") != null) {
                CTMap.put("value", m1.get(name + "CT费").get("value"));
                CTcMap.put("value", m1.get(name + "CT费").get("count"));
            }
            map.put("CTqiuhe_5", CTMap);
            map.put("CTjicishu_1", CTcMap);

            Map<String, Object> TCDMap = new HashMap<>();
            TCDMap.put("unit", "元");
            TCDMap.put("value", 0);
            Map<String, Object> TCDcMap = new HashMap<>();
            TCDcMap.put("unit", "次");
            TCDcMap.put("value", 0);

            if (m1.get(name + "TCD") != null)
                TCDMap.put("value", m1.get(name + "TCD").get("value"));
            map.put("TCDqiuhe_5", TCDMap);
            if (m1.get(name + "TCD") != null)
                TCDcMap.put("value", m1.get(name + "TCD").get("count"));
            map.put("TCDjicishu_1", TCDcMap);

            Map<String, Object> bingliMap = new HashMap<>();
            bingliMap.put("unit", "元");
            bingliMap.put("value", 0);
            Map<String, Object> bcMap = new HashMap<>();
            bcMap.put("unit", "次");
            bcMap.put("value", 0);

            if (m1.get(name + "病理费") != null)
                bingliMap.put("value", m1.get(name + "病理费").get("value"));
            map.put("bingliqiuhe_5", bingliMap);
            if (m1.get(name + "病理费") != null)
                bcMap.put("value", m1.get(name + "病理费").get("count"));
            map.put("binglijicishu_1", bcMap);

            Map<String, Object> cailiaoMap = new HashMap<>();
            cailiaoMap.put("unit", "元");
            cailiaoMap.put("value", 0);
            Map<String, Object> caicMap = new HashMap<>();
            caicMap.put("unit", "次");
            caicMap.put("value", 0);

            if (m1.get(name + "材料费") != null)
                cailiaoMap.put("value", m1.get(name + "材料费").get("value"));
            map.put("cailiaoqiuhe_5", cailiaoMap);
            if (m1.get(name + "材料费") != null)
                caicMap.put("value", m1.get(name + "材料费").get("count"));
            map.put("cailiaojicishu_1", caicMap);

            Map<String, Object> caichaoMap = new HashMap<>();
            caichaoMap.put("unit", "元");
            caichaoMap.put("value", 0);
            Map<String, Object> cailcMap = new HashMap<>();
            cailcMap.put("unit", "次");
            cailcMap.put("value", 0);

            if (m1.get(name + "彩超费") != null)
                caichaoMap.put("value", m1.get(name + "彩超费").get("value"));
            map.put("caichaoqiuhe_5", caichaoMap);
            if (m1.get(name + "彩超费") != null)
                cailcMap.put("value", m1.get(name + "彩超费").get("count"));
            map.put("caichaojicishu_1", cailcMap);


            Map<String, Object> caoyaoMap = new HashMap<>();
            caoyaoMap.put("unit", "元");
            caoyaoMap.put("value", 0);
            Map<String, Object> caocMap = new HashMap<>();
            caocMap.put("unit", "次");
            caocMap.put("value", 0);

            if (m1.get(name + "草药费") != null)
                caoyaoMap.put("value", m1.get(name + "草药费").get("value"));
            map.put("caoyaoqiuhe_5", caoyaoMap);
            if (m1.get(name + "草药费") != null)
                caocMap.put("value", m1.get(name + "草药费").get("count"));
            map.put("caoyaojicishu_1", caocMap);


            Map<String, Object> zhongchengMap = new HashMap<>();
            zhongchengMap.put("unit", "元");
            zhongchengMap.put("value", 0);
            Map<String, Object> zcMap = new HashMap<>();
            zcMap.put("unit", "次");
            zcMap.put("value", 0);


            if (m1.get(name + "中成药费") != null)
                zhongchengMap.put("value", m1.get(name + "中成药费").get("value"));
            map.put("zhongchengyaoqiuhe_5", zhongchengMap);
            if (m1.get(name + "中成药费") != null)
                zcMap.put("value", m1.get(name + "中成药费").get("count"));
            map.put("zhongchengyaojicishu_1", zcMap);


            Map<String, Object> ciMap = new HashMap<>();
            ciMap.put("unit", "元");
            ciMap.put("value", 0);
            Map<String, Object> cicMap = new HashMap<>();
            cicMap.put("unit", "次");
            cicMap.put("value", 0);

            if (m1.get(name + "磁共振") != null)
                ciMap.put("value", m1.get(name + "磁共振").get("value"));
            map.put("cigongzhenqiuhe_5", ciMap);
            if (m1.get(name + "磁共振") != null)
                cicMap.put("value", m1.get(name + "磁共振").get("count"));
            map.put("cigongzhenjicishu_1", cicMap);


            Map<String, Object> huMap = new HashMap<>();
            huMap.put("unit", "元");
            huMap.put("value", 0);
            Map<String, Object> hucMap = new HashMap<>();
            hucMap.put("unit", "次");
            hucMap.put("value", 0);

            if (m1.get(name + "护理费") != null)
                huMap.put("value", m1.get(name + "护理费").get("value"));
            map.put("huliqiuhe_5", huMap);
            if (m1.get(name + "护理费") != null)
                hucMap.put("value", m1.get(name + "护理费").get("count"));
            map.put("hulijicishu_1", hucMap);

            Map<String, Object> huaMap = new HashMap<>();
            huaMap.put("unit", "元");
            huaMap.put("value", 0);
            Map<String, Object> huacMap = new HashMap<>();
            huacMap.put("unit", "次");
            huacMap.put("value", 0);


            if (m1.get(name + "化验费") != null)
                huaMap.put("value", m1.get(name + "化验费").get("value"));
            map.put("huayanqiuhe_5", huaMap);
            if (m1.get(name + "化验费") != null)
                huacMap.put("value", m1.get(name + "化验费").get("count"));
            map.put("huayanjicishu_1", huacMap);

            Map<String, Object> huanMap = new HashMap<>();
            huanMap.put("unit", "元");
            huanMap.put("value", 0);
            Map<String, Object> huancMap = new HashMap<>();
            huancMap.put("unit", "次");
            huancMap.put("value", 0);

            if (m1.get(name + "换药费") != null)
                huanMap.put("value", m1.get(name + "换药费").get("value"));
            map.put("huanyaoqiuhe_5", huanMap);
            if (m1.get(name + "换药费") != null)
                huancMap.put("value", m1.get(name + "换药费").get("count"));
            map.put("huanyaojicishu_1", huancMap);

            Map<String, Object> jianMap = new HashMap<>();
            jianMap.put("unit", "元");
            jianMap.put("value", 0);
            Map<String, Object> jiancMap = new HashMap<>();
            jiancMap.put("unit", "次");
            jiancMap.put("value", 0);

            if (m1.get(name + "检查费") != null)
                jianMap.put("value", m1.get(name + "检查费").get("value"));
            map.put("jianchaqiuhe_5", jianMap);
            if (m1.get(name + "检查费") != null)
                jiancMap.put("value", m1.get(name + "检查费").get("count"));
            map.put("jianchajicishu_1", jiancMap);

            Map<String, Object> jiuMap = new HashMap<>();
            jiuMap.put("unit", "元");
            jiuMap.put("value", 0);
            Map<String, Object> jiucMap = new HashMap<>();
            jiucMap.put("unit", "次");
            jiucMap.put("value", 0);

            if (m1.get(name + "救护车") != null)
                jiuMap.put("value", m1.get(name + "救护车").get("value"));
            map.put("jiuhucheqiuhe_5", jiuMap);
            if (m1.get(name + "救护车") != null)
                jiucMap.put("value", m1.get(name + "救护车").get("count"));
            map.put("jiuhuchejicishu_1", jiucMap);

            Map<String, Object> liliaoMap = new HashMap<>();
            liliaoMap.put("unit", "元");
            liliaoMap.put("value", 0);
            Map<String, Object> liliaocMap = new HashMap<>();
            liliaocMap.put("unit", "次");
            liliaocMap.put("value", 0);

            if (m1.get(name + "理疗车") != null)
                liliaoMap.put("value", m1.get(name + "理疗费").get("value"));
            map.put("liliaoqiuhe_5", liliaoMap);
            if (m1.get(name + "理疗车") != null)
                liliaocMap.put("value", m1.get(name + "理疗费").get("count"));
            map.put("liliaojicishu_1", liliaocMap);

            Map<String, Object> maMap = new HashMap<>();
            maMap.put("unit", "元");
            maMap.put("value", 0);
            Map<String, Object> macMap = new HashMap<>();
            macMap.put("unit", "次");
            macMap.put("value", 0);

            if (m1.get(name + "麻醉费") != null)
                maMap.put("value", m1.get(name + "麻醉费").get("value"));
            map.put("mazuiqiuhe_5", maMap);
            if (m1.get(name + "麻醉费") != null)
                macMap.put("value", m1.get(name + "麻醉费").get("count"));
            map.put("mazuijicishu_1", macMap);

            Map<String, Object> naoMap = new HashMap<>();
            naoMap.put("unit", "元");
            naoMap.put("value", 0);
            Map<String, Object> naocMap = new HashMap<>();
            naocMap.put("unit", "次");
            naocMap.put("value", 0);

            if (m1.get(name + "脑电费") != null)
                naoMap.put("value", m1.get(name + "脑电费").get("value"));
            map.put("naodiantuqiuhe_5", naoMap);
            if (m1.get(name + "脑电费") != null)
                naocMap.put("value", m1.get(name + "脑电费").get("count"));
            map.put("naodiantujicishu_1", naocMap);

            Map<String, Object> paiMap = new HashMap<>();
            paiMap.put("unit", "元");
            paiMap.put("value", 0);
            Map<String, Object> paicMap = new HashMap<>();
            paicMap.put("unit", "次");
            paicMap.put("value", 0);

            if (m1.get(name + "拍片费") != null)
                paiMap.put("value", m1.get(name + "拍片费").get("value"));
            map.put("paipianqiuhe_5", paiMap);
            if (m1.get(name + "拍片费") != null)
                paicMap.put("value", m1.get(name + "拍片费").get("count"));
            map.put("paipianjicishu_1", paicMap);

            Map<String, Object> ssMap = new HashMap<>();
            ssMap.put("unit", "元");
            ssMap.put("value", 0);
            Map<String, Object> sscMap = new HashMap<>();
            sscMap.put("unit", "次");
            sscMap.put("value", 0);

            if (m1.get(name + "手术材料费") != null)
                ssMap.put("value", m1.get(name + "手术材料费").get("value"));
            map.put("shoushucailiaoqiuhe_5", ssMap);
            if (m1.get(name + "手术材料费") != null)
                sscMap.put("value", m1.get(name + "手术材料费").get("count"));
            map.put("shoushucailiaojicishu_1", sscMap);

            Map<String, Object> shouMap = new HashMap<>();
            shouMap.put("unit", "元");
            shouMap.put("value", 0);
            Map<String, Object> shoucMap = new HashMap<>();
            shoucMap.put("unit", "次");
            shoucMap.put("value", 0);


            if (m1.get(name + "手术费") != null)
                shouMap.put("value", m1.get(name + "手术费").get("value"));
            map.put("shoushuqiuhe_5", shouMap);
            if (m1.get(name + "手术费") != null)
                shoucMap.put("value", m1.get(name + "手术费").get("count"));
            map.put("shoushujicishu_1", shoucMap);

            Map<String, Object> tiMap = new HashMap<>();
            tiMap.put("unit", "元");
            tiMap.put("value", 0);
            Map<String, Object> ticMap = new HashMap<>();
            ticMap.put("unit", "次");
            ticMap.put("value", 0);

            if (m1.get(name + "体检费") != null)
                tiMap.put("value", m1.get(name + "体检费").get("value"));
            map.put("tijianqiuhe_5", tiMap);
            if (m1.get(name + "体检费") != null)
                ticMap.put("value", m1.get(name + "体检费").get("count"));
            map.put("tijianjicishu_1", ticMap);


            Map<String, Object> weiMap = new HashMap<>();
            weiMap.put("unit", "元");
            weiMap.put("value", 0);
            Map<String, Object> weicMap = new HashMap<>();
            weicMap.put("unit", "次");
            weicMap.put("value", 0);

            if (m1.get(name + "胃镜费") != null)
                weiMap.put("value", m1.get(name + "胃镜费").get("value"));
            map.put("weijingqiuhe_5", weiMap);
            if (m1.get(name + "胃镜费") != null)
                weicMap.put("value", m1.get(name + "胃镜费").get("count"));
            map.put("weijingjicishu_1", weicMap);

            Map<String, Object> xiMap = new HashMap<>();
            xiMap.put("unit", "元");
            xiMap.put("value", 0);
            Map<String, Object> xicMap = new HashMap<>();
            xicMap.put("unit", "次");
            xicMap.put("value", 0);

            if (m1.get(name + "西药费") != null)
                xiMap.put("value", m1.get(name + "西药费").get("value"));
            map.put("xiyaoqiuhe_5", xiMap);
            if (m1.get(name + "西药费") != null)
                xicMap.put("value", m1.get(name + "西药费").get("count"));
            map.put("xiyaojicishu_1", xicMap);

            Map<String, Object> xinMap = new HashMap<>();
            xinMap.put("unit", "元");
            xinMap.put("value", 0);
            Map<String, Object> xincMap = new HashMap<>();
            xincMap.put("unit", "次");
            xincMap.put("value", 0);

            if (m1.get(name + "心超费") != null)
                xinMap.put("value", m1.get(name + "心超费").get("value"));
            map.put("xinchaoqiuhe_5", xinMap);
            if (m1.get(name + "心超费") != null)
                xincMap.put("value", m1.get(name + "心超费").get("count"));
            map.put("xinchaojicishu_1", xincMap);

            Map<String, Object> zhenMap = new HashMap<>();
            zhenMap.put("unit", "元");
            zhenMap.put("value", 0);
            Map<String, Object> zhencMap = new HashMap<>();
            zhencMap.put("unit", "次");
            zhencMap.put("value", 0);

            if (m1.get(name + "诊疗费") != null)
                zhenMap.put("value", m1.get(name + "诊疗费").get("value"));
            map.put("zhenliaoqiuhe_5", zhenMap);
            if (m1.get(name + "诊疗费") != null)
                zhencMap.put("value", m1.get(name + "诊疗费").get("count"));
            map.put("zhenliaojicishu_1", zhencMap);

            Map<String, Object> zhiMap = new HashMap<>();
            zhiMap.put("unit", "元");
            zhiMap.put("value", 0);
            Map<String, Object> zhicMap = new HashMap<>();
            zhicMap.put("unit", "次");
            zhicMap.put("value", 0);

            if (m1.get(name + "治疗费") != null)
                zhiMap.put("value", m1.get(name + "治疗费").get("value"));
            map.put("zhiliaoqiuhe_5", zhiMap);
            if (m1.get(name + "治疗费") != null)
                zhicMap.put("value", m1.get(name + "治疗费").get("count"));
            map.put("zhiliaojicishu_1", zhicMap);

            Map<String, Object> zhuMap = new HashMap<>();
            zhuMap.put("unit", "元");
            zhuMap.put("value", 0);
            Map<String, Object> zhucMap = new HashMap<>();
            zhucMap.put("unit", "次");
            zhucMap.put("value", 0);

            if (m1.get(name + "注射费") != null)
                zhuMap.put("value", m1.get(name + "注射费").get("value"));
            map.put("zhusheqiuhe_5", zhuMap);
            if (m1.get(name + "注射费") != null)
                zhucMap.put("value", m1.get(name + "注射费").get("count"));
            map.put("zhushejicishu_1", zhucMap);


            int jiucishu = 0;
            Map jiuzhen = new HashMap();
            jiuzhen.put("unit", "次");
            jiuzhen.put("value", m2.get(name).get("count"));
            map.put("jiuzhencishushu_1", jiuzhen);


            Map ks = new HashMap();
            ks.put("unit", "个");
            int jzl = 0;
            if (fenliejiesuan.get(name).get(0).get("chuyuankeshimingcheng_2") == null) {
                jzl = 1;
            } else if (fenliejiesuan.get(name).get(0).get("chuyuankeshimingcheng_2").equals(fenliejiesuan.get(name).get(0).get("ruyuankeshimingcheng_2"))) {
                jzl = 1;
            } else {
                jzl = 2;
            }
            ks.put("value", jzl);
            map.put("jiuzhenkeshishuliang_1", ks);

            Map hz = new HashMap();
            hz.put("unit", "种");
            int zl = 0;
            if (fenliejiesuan.get(name).get(0).get("chuyuanjibingzhenduanbianma_3") == null) {
                zl = 1;
            } else if (fenliejiesuan.get(name).get(0).get("chuyuanjibingzhenduanbianma_3").equals(fenliejiesuan.get(name).get(0).get("ruyuanjibingzhenduanbianma_3"))) {
                zl = 1;
            } else {
                zl = 2;
            }
            hz.put("value", zl);
            map.put("huanbingzhonglei_1", hz);

            Map jiuday = new HashMap();
            jiuday.put("unit", "次");
            int day = 0;
            if (jiucishu > 1
                    && !fenliejiesuan.get(name).get(0).get("chuyuanriqi_2").equals("")
                    && fenliejiesuan.get(name).size() >= 2
                    && !fenliejiesuan.get(name).get(1).get("ruyuanriqi_2").equals("")
                    && !fenliejiesuan.get(name).get(0).get("ruyuanriqi_2").equals("")) {
                Long d1 = (Long) fenliejiesuan.get(name).get(1).get("ruyuanriqi_2");
                Long d2 = (Long) fenliejiesuan.get(name).get(0).get("ruyuanriqi_2");
                day = (int) (d1 - d2) / (24 * 60 * 60 * 1000);
                day = day > 0 ? day : -day;

                jiuday.put("value", day);
                map.put("jiuzhenjiange_1", jiuday);
            } else {
                jiuday.put("value", 0);
            }
            map.put("jiuzhenjiange_1", jiuday);

            Map mkzl = new HashMap();
            mkzl.put("unit", "种");
            mkzl.put("value", 0);
            map.put("menzhenkoufuyaozhonglei_1", mkzl);

            Map mzkj = new HashMap();
            mzkj.put("unit", "元");
            mzkj.put("value", 0);
            map.put("menzhenkoufuyaojine_1", mzkj);

            ArrayList<String> jiancha = new ArrayList<>();

            jiancha.add("CT费");
            jiancha.add("TCD");
            jiancha.add("彩超费");
            jiancha.add("磁共振");
            jiancha.add("化验费");
            jiancha.add("检查费");
            jiancha.add("脑电图");
            jiancha.add("拍片费");
            jiancha.add("胃镜费");
            jiancha.add("心超费");


            Map mjcj = new HashMap();
            Map mjcs = new HashMap();
            mjcj.put("unit", "元");
            mjcs.put("unit", "个");
            Double mjcMoney = 0.0;
            int mjccount = 0;
            for (Map<String, Map> k : mappedResults) {
                if (k.get("_id").get("xingming_104").equals(name)
                        && jiancha.contains(k.get("_id").get("shoufeixiangmuleibie_5"))) {
                    Object m = k.get("value");
                    Object c = k.get("count");
                    if (m instanceof Double) {
                        mjcMoney = (Double) m;
                    } else {
                        mjcMoney = (Integer) m + 0.0;
                    }
                    mjccount += (Integer) c;
                }
            }
            mjcj.put("value", mjcMoney);
            map.put("menzhenjianchajine_1", mjcj);
            mjcs.put("value", mjccount);
            map.put("menzhenjianchaxiangmushuliang_1", mjcs);

            Map zycs = new HashMap();
            Map zyts = new HashMap();
            zyts.put("unit", "天");
            zycs.put("unit", "次");

            zycs.put("value", m3.get(name).get("count"));
            zyts.put("value", m3.get(name).get("value"));
            map.put("zhuyuancishushu_1", zycs);
            map.put("zhuyuantianshu_7", zyts);

            Map nianl = new HashMap<>();
            if (!map.get("nianling_27").equals("")) {
                nianl = (Map) map.get("nianling_27");
            }

            if ((Integer) nianl.get("value") < 0) {
                map.put("nianlingzu_1", "非法");
            }
            if ((Integer) nianl.get("value") <= 6) {
                map.put("nianlingzu_1", "婴幼儿");
            } else if ((Integer) nianl.get("value") <= 12) {
                map.put("nianlingzu_1", "少儿");
            } else if ((Integer) nianl.get("value") <= 17) {
                map.put("nianlingzu_1", "青少年");
            } else if ((Integer) nianl.get("value") <= 45) {
                map.put("nianlingzu_1", "青年");
            } else if ((Integer) nianl.get("value") <= 69) {
                map.put("nianlingzu_1", "中年");
            } else if ((Integer) nianl.get("value") > 69) {
                map.put("nianlingzu_1", "老年");
            } else {
                map.put("nianlingzu_1", "非法");
            }

            map.put("teshubaoxiao_1", "");
            map.put("pinxue_1", "");
            map.put("jingshenbing_1", "");
            map.put("bendiyidi_1", "");
            map.put("diqujiedaoxiangzhen_1", fenliejiesuan.get(name).get(0).get("danweimingcheng_25"));
            Map<String, Object> dzje = new HashMap<>();
            dzje.put("unit", "元");
            dzje.put("value", zongjine);
            map.put("danweishijianzongjine_3", dzje);
            Map<String, Object> dyzje = new HashMap<>();
            dyzje.put("unit", "元");
            dyzje.put("value", ybzongjine);
            map.put("danweishijianyibaobaoxiaozonge_3", dyzje);

            Map<String, Object> dyzjeb = new HashMap<>();
            dyzjeb.put("unit", "比");
            dyzjeb.put("value", ybzongjine / zongjine);
            map.put("danweishijianyibaobili_3", dyzjeb);

            insertList.add(map);
            fenliejiesuan.get(name).clear();
            iii++;
            if (insertList.size() >= 1000) {
                mongoTemplate.insert(insertList, huanzhebiaoku);
                insertList = new ArrayList<>();
                System.out.println(iii);
            }
        }
        if (insertList.size() > 0) {
            mongoTemplate.insert(insertList, huanzhebiaoku);
            insertList = new ArrayList<>();
        }
        System.out.println("成功");
    }


    @Test
    void yiyuanbiaodaoru() {
        String huanzhebiaoku = "com.ns.entity.object.form.instance.nsjgpv79iu55j528ig78.huanzhebiao_1";
        String yishengbiaoku = "com.ns.entity.object.form.instance.nsjgpv79iu55j528ig78.yishengbiao_1";
        String jiesuanku = "com.ns.entity.object.form.instance.nsjgpv79iu55j528ig78.feiyongjiesuanxinxi_1";
        String mingxiku = "com.ns.entity.object.form.instance.nsjgpv79iu55j528ig78.feiyongmingxixinxi_1";
        String yiyuanbiaoku = "com.ns.entity.object.form.instance.nsjgpv79iu55j528ig78.yiyuanbiao_1";

        TypedAggregation<Map> TypedAggregation = Aggregation.newAggregation(
                Map.class,
                Aggregation.group().sum("yiliaofeizonge_6.value").as("value"));
        AggregationResults<Map> aggregationResults = mongoTemplate.aggregate(
                TypedAggregation,
                jiesuanku,
                Map.class);
        List<Map> mappedResults = aggregationResults.getMappedResults();

        TypedAggregation<Map> TypedAggregation2 = Aggregation.newAggregation(
                Map.class,
                Aggregation.group().sum("tongchouzhifujine_5.value").as("value"));
        AggregationResults<Map> aggregationResults2 = mongoTemplate.aggregate(
                TypedAggregation2,
                jiesuanku,
                Map.class);
        List<Map> mappedResults2 = aggregationResults2.getMappedResults();

        TypedAggregation<Map> TypedAggregation3 = Aggregation.newAggregation(
                Map.class,
                Aggregation.group().sum("danweishijianneijiezhenrenci_1.value").as("value"));
        AggregationResults<Map> aggregationResults3 = mongoTemplate.aggregate(
                TypedAggregation3,
                yishengbiaoku,
                Map.class);
        List<Map> mappedResults3 = aggregationResults3.getMappedResults();

        TypedAggregation<Map> TypedAggregation4 = Aggregation.newAggregation(
                Map.class,
                Aggregation.group("shoufeixiangmuleibie_5").count().as("count"));
        AggregationResults<Map> aggregationResults4 = mongoTemplate.aggregate(
                TypedAggregation4,
                mingxiku,
                Map.class);
        List<Map> mappedResults4 = aggregationResults4.getMappedResults();


        Map<String, Object> map = new HashMap<>();
        //系统字段
        String mainId = UUID.randomUUID().toString();
        map.put("_id", mainId);
        map.put("create_time", System.currentTimeMillis());
        map.put("create_account", "admin");
        map.put("category_id", "32b8c105-afaa-4ba3-adb6-ae3788f0ec07");
        map.put("data_status", "已归档");
        map.put("data_type", 1);
        map.put("priority", "");
        map.put("bind_id", mainId);
        map.put("corp_id", "nsrcu88p7uy22m7i9ioz");
        map.put("parent_corp_id_list", new ArrayList<>());
        map.put("bind_category_id", "");

        //业务固定字段
        map.put("yiyuanmingcheng_15", "孝感市第一人民医院");
        map.put("yiyuanleibie_13", "公立医院");
        map.put("yiyuandengji_13", "三级");
        map.put("yiyuanxingzhi_13", "");
        map.put("yibaoniandu_15", "2023");

        //患者人数
        Long huanzhenum = mongoTemplate.count(new Query(), huanzhebiaoku);
        Map huanmap = new HashMap<>();
        huanmap.put("unit", "个");
        huanmap.put("value", huanzhenum);
        map.put("huanzherenshu_3", huanmap);
        //金额
        Double jine = Double.parseDouble(mappedResults.get(0).get("value").toString());
        Map jinemap = new HashMap<>();
        jinemap.put("unit", "元");
        jinemap.put("value", jine);
        map.put("jine_24", jinemap);
        map.put("danweishijianzongjine_6", jinemap);

        //医保金额
        Double ybjine = Double.parseDouble(mappedResults2.get(0).get("value").toString());
        Map ybjinemap = new HashMap<>();
        ybjinemap.put("unit", "元");
        ybjinemap.put("value", ybjine);
        map.put("danweishijianyibaobaoxiaozonge_6", ybjinemap);

        //医保比例
        Map ybbili = new HashMap<>();
        ybbili.put("unit", "比");
        ybbili.put("value", ybjine / jine);
        map.put("danweishijianyibaobili_6", ybbili);

        //科室数量
        Map ksmap = new HashMap<>();
        ksmap.put("unit", "个");
        ksmap.put("value", 98);
        map.put("danweishijiankeshishuliang_4", ksmap);

        //医生数量
        Map yishengmap = new HashMap<>();
        yishengmap.put("unit", "个");
        yishengmap.put("value", 308);
        map.put("danweishijianyishengshuliang_4", yishengmap);

        //床位数量
        Map cwmap = new HashMap<>();
        cwmap.put("unit", "个");
        cwmap.put("value", 0);
        map.put("danweishijianchuangweishu_4", cwmap);
        //本异地比
        Map bymap = new HashMap<>();
        bymap.put("unit", "比");
        bymap.put("value", 0);
        map.put("danweishijianbendiyidibili_4", bymap);
        //接诊人次
        Integer jiezhennum = Integer.parseInt(mappedResults3.get(0).get("value").toString());
        Map jiezhenmap = new HashMap<>();
        jiezhenmap.put("unit", "次");
        jiezhenmap.put("value", jiezhennum);
        map.put("danweishijianneijiezhenrenci_4", jiezhenmap);

        //住院人次

        Map zhuyuanmap = new HashMap<>();
        zhuyuanmap.put("unit", "次");
        zhuyuanmap.put("value", 41652);
        map.put("danweishijianzhuyuanrenci_4", zhuyuanmap);

        //手术人次
        Map shoushumap = new HashMap<>();
        shoushumap.put("unit", "次");
        shoushumap.put("value", 0);
        for (Map k : mappedResults4) {
            if (k.get("_id").toString().equals("手术费")) {
                shoushumap.put("value", k.get("count"));
            }
        }
        map.put("danweishijianshoushurenci_4", shoushumap);
        //单位时间口服药种类
        Map kfzmap = new HashMap<>();
        kfzmap.put("unit", "种");
        kfzmap.put("value", 0);
        map.put("danweishijiankoufuyaozhonglei_4", kfzmap);
        //单位时间口服药金额
        Map kfjmap = new HashMap<>();
        kfjmap.put("unit", "元");
        kfjmap.put("value", 0);
        map.put("danweishijiankoufuyaojine_2", kfjmap);
        //单位时间最大金额
        Map dwjmap = new HashMap<>();
        dwjmap.put("unit", "元");
        dwjmap.put("value", 0);
        map.put("danweishijianzuidajine_2", dwjmap);

        //单位时间最大医生数量
        Map dzymap = new HashMap<>();
        dzymap.put("unit", "个");
        dzymap.put("value", 0);
        map.put("danweishijianzuidayishengshuliang_2", dzymap);
        //单位时间最小医生数量
        Map dzyxmap = new HashMap<>();
        dzyxmap.put("unit", "个");
        dzyxmap.put("value", 0);
        map.put("danweishijianzuixiaoyishengshuliang_2", dzyxmap);
        //最大住院在院人数
        Map zdzmap = new HashMap<>();
        zdzmap.put("unit", "个");
        zdzmap.put("value", 0);
        map.put("zuidazhuyuanzaiyuanrenshu_2", zdzmap);
        //最大日门诊人次
        Map zdmmap = new HashMap<>();
        zdmmap.put("unit", "次");
        zdmmap.put("value", 0);
        map.put("zuidarimenzhenrenci_2", zdmmap);
        //最大日手术人次
        Map zssmap = new HashMap<>();
        zssmap.put("unit", "次");
        zssmap.put("value", 0);
        map.put("zuidarishoushurenci_2", zssmap);
        //最大日开口服药数量
        Map zdkmap = new HashMap<>();
        zdkmap.put("unit", "个");
        zdkmap.put("value", 0);
        map.put("zuidarikaikoufuyaoshuliang_2", zdkmap);
        //最大日开口服药金额
        Map zdjmap = new HashMap<>();
        zdjmap.put("unit", "元");
        zdjmap.put("value", 0);
        map.put("zuidarikaikoufuyaojine_2", zdjmap);

        mongoTemplate.insert(map, yiyuanbiaoku);
        System.out.println("ok");
    }


    @Test
    void yaopinclear() {
        String yaopinbiaoku = "com.ns.entity.object.form.instance.nsjgpv79iu55j528ig78.yaopinbiao_1";
        List<Map> all = mongoTemplate.findAll(Map.class, yaopinbiaoku);
        if (all.size() > 0) {
            mongoTemplate.remove(new Query(), yaopinbiaoku);
            // System.out.println(all);
        }
    }

    @Test
    void yaopinbiaodaoru() {
        String yaopinbiaoku = "com.ns.entity.object.form.instance.nsjgpv79iu55j528ig78.yaopinbiao_1";
        String yishengbiaoku = "com.ns.entity.object.form.instance.nsjgpv79iu55j528ig78.yishengbiao_1";
        String jiesuanku = "com.ns.entity.object.form.instance.nsjgpv79iu55j528ig78.feiyongjiesuanxinxi_1";
        String mingxiku = "com.ns.entity.object.form.instance.nsjgpv79iu55j528ig78.feiyongmingxixinxi_1";


        TypedAggregation<Map> TypedAggregation = Aggregation.newAggregation(
                Map.class,
                Aggregation.group("yibaomulubianma_5", "yibaomulumingcheng_6")
                        .count().as("count")
                        .sum("jine_20.value").as("jine")
                        .sum("shuliang_42.value").as("shul")
        );
        AggregationResults<Map> aggregationResults = mongoTemplate.aggregate(
                TypedAggregation,
                mingxiku,
                Map.class);
        List<Map> mappedResults = aggregationResults.getMappedResults();

        TypedAggregation<Map> TypedAggregation2 = Aggregation.newAggregation(
                Map.class,
                Aggregation.group("yibaomulubianma_5", "keshimingcheng_26")
        );
        AggregationResults<Map> aggregationResults2 = mongoTemplate.aggregate(
                TypedAggregation2,
                mingxiku,
                Map.class);
        List<Map> mappedResults2 = aggregationResults2.getMappedResults();

        Map<String, String> bmksmap = new HashMap<>();
        for (Map<String, Map> k : mappedResults2) {
            if (k.get("_id").get("yibaomulubianma_5") != null && k.get("_id").get("keshimingcheng_26") != null)
                bmksmap.put(k.get("_id").get("yibaomulubianma_5").toString(), k.get("_id").get("keshimingcheng_26").toString());
        }

        Map<String, String> bianmamap = new HashMap<>();
        for (Map<String, Map> k : mappedResults) {
            bianmamap.put(k.get("_id").get("yibaomulubianma_5").toString(), k.get("_id").get("yibaomulumingcheng_6").toString());
        }

        Map<String, Map<String, Map>> m1 = new HashMap<>();


        for (Map<String, Map> k : mappedResults) {
            if (k.get("_id").get("yibaomulubianma_5") != null && k.get("_id").get("yibaomulumingcheng_6") != null)
                m1.put(k.get("_id").get("yibaomulubianma_5").toString()
                        + ":" + k.get("_id").get("yibaomulumingcheng_6").toString(), k);
        }
        int ii = 0;
        List insertlist = new ArrayList();
        for (String bm : bianmamap.keySet()) {
            if (m1.get(bm + ":" + bianmamap.get(bm)) == null) {
                continue;
            }
            Map<String, Object> map = new HashMap<>();
            //系统字段
            String mainId = UUID.randomUUID().toString();
            map.put("_id", mainId);
            map.put("create_time", System.currentTimeMillis());
            map.put("create_account", "admin");
            map.put("category_id", "37f621a3-2718-4941-a2ca-3141c27f055f");
            map.put("data_status", "已归档");
            map.put("data_type", 1);
            map.put("priority", "");
            map.put("bind_id", mainId);
            map.put("corp_id", "nsrcu88p7uy22m7i9ioz");
            map.put("parent_corp_id_list", new ArrayList<>());
            map.put("bind_category_id", "");

            map.put("yiyuanmingcheng_7", "孝感市第一人民医院");
            map.put("yiyuanleibie_5", "公立医院");
            map.put("yiyuandengji_5", "三级");
            map.put("yiyuanxingzhi_5", "");
            map.put("yibaoniandu_7", 2023);
            map.put("yibaomulubianma_7", bm);
            map.put("yibaomulumingcheng_8", bianmamap.get(bm));


            //科室名称
            if (bmksmap.get(bm) != null)
                map.put("keshimingcheng_29", bmksmap.get(bm));

            //患者人数
            Map huanz = new HashMap<>();
            huanz.put("unit", "个");
            huanz.put("value", 0);
            if (m1.get(bm + ":" + bianmamap.get(bm)).get("count") != null)
                huanz.put("value", m1.get(bm + ":" + bianmamap.get(bm)).get("count"));
            map.put("huanzherenshu_1", huanz);

            //剂量
            Map jil = new HashMap<>();
            jil.put("unit", "个");
            jil.put("value", 0);
            if (m1.get(bm + ":" + bianmamap.get(bm)).get("shul") != null)
                jil.put("value", m1.get(bm + ":" + bianmamap.get(bm)).get("shul"));
            map.put("jiliang_3", jil);

            //金额
            Map jin = new HashMap<>();
            jin.put("unit", "元");
            jin.put("value", 0);
            if (m1.get(bm + ":" + bianmamap.get(bm)).get("jine") != null)
                jin.put("value", m1.get(bm + ":" + bianmamap.get(bm)).get("jine"));
            map.put("jine_22", jin);
            insertlist.add(map);
            ii++;
            if (insertlist.size() >= 1000) {
                mongoTemplate.insert(insertlist, yaopinbiaoku);
                insertlist = new ArrayList();
                System.out.println(ii);
            }
        }
        if (insertlist.size() > 0) {
            mongoTemplate.insert(insertlist, yaopinbiaoku);
            insertlist = new ArrayList();
            System.out.println("存");
        }
        System.out.println("ok");
    }

    @Test
    void zhenliaobiaodaoru() {
        String zhenliaoku = "com.ns.entity.object.form.instance.nsjgpv79iu55j528ig78.zhenliaobiao_1";
        String jiesuanku = "com.ns.entity.object.form.instance.nsjgpv79iu55j528ig78.feiyongjiesuanxinxi_1";
        String mingxiku = "com.ns.entity.object.form.instance.nsjgpv79iu55j528ig78.feiyongmingxixinxi_1";


        Integer iii = 0;
        TypedAggregation<Map> TypedAggregation3 = Aggregation.newAggregation(
                Map.class,
                Aggregation.group("danjuhao_13").addToSet("yibaomulubianma_5").as("m3")
        ).withOptions(Aggregation.newAggregationOptions().allowDiskUse(true).build());

        AggregationResults<Map> aggregationResults3 = mongoTemplate.aggregate(
                TypedAggregation3,
                mingxiku,
                Map.class);
        List<Map> mappedResults3 = aggregationResults3.getMappedResults();

        Map<String, Integer> m3 = new HashMap<>();
        for (Map k : mappedResults3) {
            List s = (List) k.get("m3");
            m3.put(k.get("_id").toString(), s.size());
        }

        Query q1 = new Query();
        q1.fields().include("xingming_103").include("chushengriqi_28")
                .include("nianling_23").include("xingbie_22")
                .include("xianzhongleixing_2").include("danweimingcheng_25")
                .include("ruyuankeshimingcheng_2").include("zhuzhiyishengdaima_1")
                .include("zhuzhiyisheng_1").include("ruyuanjibingmingcheng_3")
                .include("chuyuanjibingmingcheng_3").include("ruyuanriqi_2")
                .include("chuyuanriqi_2").include("zhuyuantianshu_3")
                .include("danjuhao_12").include("chuyuankeshimingcheng_2")
                .exclude("_id");

        List<Map> jiehuanzhe = mongoTemplate.find(q1, Map.class, jiesuanku);

        Map<String, List<Map>> fenliejiesuan = new HashMap<>();
        for (Map k : jiehuanzhe) {
            String fieldValue = k.get("danjuhao_12").toString(); // 假设用于分组的字段名为 "fieldName"
            if (fenliejiesuan.get(fieldValue) == null) {
                fenliejiesuan.put(fieldValue, new ArrayList<>());
            }
            fenliejiesuan.get(fieldValue).add(k);
        }
        jiehuanzhe.clear();


        TypedAggregation<Map> TypedAggregation = Aggregation.newAggregation(
                Map.class,
                Aggregation.group("danjuhao_12")
        ).withOptions(Aggregation.newAggregationOptions().allowDiskUse(true).build());
        ;

        AggregationResults<Map> aggregationResults = mongoTemplate.aggregate(
                TypedAggregation,
                jiesuanku,
                Map.class);
        List<Map> mappedResults = aggregationResults.getMappedResults();

        Map<String, Map> m2 = new HashMap<>();
        for (Map k : mappedResults) {
            m2.put(k.get("_id").toString(), k);
        }


        TypedAggregation<Map> TypedAggregation2 = Aggregation.newAggregation(
                Map.class,
                Aggregation.group("danjuhao_13", "shoufeixiangmuleibie_5")
                        .sum("jine_20.value").as("value")
                        .count().as("count")
        ).withOptions(Aggregation.newAggregationOptions().allowDiskUse(true).build());
        ;

        AggregationResults<Map> aggregationResults2 = mongoTemplate.aggregate(
                TypedAggregation2,
                mingxiku,
                Map.class);
        List<Map> mappedResults2 = aggregationResults2.getMappedResults();

        Map<String, Map<String, Map>> m1 = new HashMap<>();
        for (Map<String, Map> k : mappedResults2) {
            m1.put(k.get("_id").get("danjuhao_13").toString()
                    + k.get("_id").get("shoufeixiangmuleibie_5").toString(), k);
        }


        List insertList = new ArrayList<>();
        for (String name : fenliejiesuan.keySet()) {

            Map<String, Object> map = new HashMap<>();
            //系统字段
            String mainId = UUID.randomUUID().toString();
            map.put("_id", mainId);
            map.put("create_time", System.currentTimeMillis());
            map.put("create_account", "admin");
            map.put("category_id", "7919a578-e29b-462e-b98f-1c198bc0a0b0");
            map.put("data_status", "已归档");
            map.put("data_type", 1);
            map.put("priority", "");
            map.put("bind_id", mainId);
            map.put("corp_id", "nsrcu88p7uy22m7i9ioz");
            map.put("parent_corp_id_list", new ArrayList<>());
            map.put("bind_category_id", "");

            //业务字段(结算)
            map.put("jiuzhenhao_21", name);
            map.put("yibaoniandu_10", 2023);
            map.put("yiyuanleibie_8", "公立医院");
            map.put("canbaorentongchouqumingcheng_7", fenliejiesuan.get(name).get(0).get("danweimingcheng_25"));
            map.put("shehuibaozhangkahao_9", "");
            map.put("zhengjianhaoma_26", "");
            map.put("xingming_107", fenliejiesuan.get(name).get(0).get("xingming_103"));
            map.put("nianling_25", fenliejiesuan.get(name).get(0).get("nianling_23"));
            map.put("xingbie_24", fenliejiesuan.get(name).get(0).get("xingbie_22"));
            map.put("danweimingcheng_27", fenliejiesuan.get(name).get(0).get("danweimingcheng_25"));
            map.put("chushengriqi_30", fenliejiesuan.get(name).get(0).get("chushengriqi_28"));
            map.put("xianzhongleixing_6", fenliejiesuan.get(name).get(0).get("xianzhongleixing_2"));
            map.put("yiyuanmingcheng_10", "孝感市第一人民医院");
            map.put("yiyuandengji_8", "三级");
            map.put("yiliaoleibie_14", fenliejiesuan.get(name).get(0).get("yiliaoleibie_8"));
            map.put("yiyuanxingzhi_8", "");
            map.put("keshimingcheng_32", "");
            map.put("zhixingkeshimingcheng_12", "");
            if (fenliejiesuan.get(name).get(0).get("ruyuankeshimingcheng_2") != null) {
                map.put("keshimingcheng_32", fenliejiesuan.get(name).get(0).get("ruyuankeshimingcheng_2"));
                map.put("zhixingkeshimingcheng_12", fenliejiesuan.get(name).get(0).get("ruyuankeshimingcheng_2"));
            }

            if (fenliejiesuan.get(name).get(0).get("chuyuankeshimingcheng_2") != null) {
                map.put("keshimingcheng_32", fenliejiesuan.get(name).get(0).get("chuyuankeshimingcheng_2"));
                map.put("zhixingkeshimingcheng_12", fenliejiesuan.get(name).get(0).get("ruyuankeshimingcheng_2"));
            }
            map.put("yishengbianma_7", fenliejiesuan.get(name).get(0).get("zhuzhiyishengdaima_1"));
            map.put("yishengxingming_5", fenliejiesuan.get(name).get(0).get("zhuzhiyisheng_1"));
            map.put("zhuzhenjibingmingcheng_5", "");
            if (fenliejiesuan.get(name).get(0).get("ruyuanjibingmingcheng_3") != null) {
                map.put("zhuzhenjibingmingcheng_5", fenliejiesuan.get(name).get(0).get("ruyuanjibingmingcheng_3"));
            }
            if (fenliejiesuan.get(name).get(0).get("chuyuanjibingmingcheng_3") != null) {
                map.put("zhuzhenjibingmingcheng_5", fenliejiesuan.get(name).get(0).get("chuyuanjibingmingcheng_3"));
            }
            if (fenliejiesuan.get(name).get(0).get("ruyuanriqi_2") != null)
                map.put("ruyuanriqi_4", fenliejiesuan.get(name).get(0).get("ruyuanriqi_2"));
            if (fenliejiesuan.get(name).get(0).get("chuyuanriqi_2") != null)
                map.put("chuyuanriqi_4", fenliejiesuan.get(name).get(0).get("chuyuanriqi_2"));
            if (fenliejiesuan.get(name).get(0).get("zhuyuantianshu_3") != null)
                map.put("zhuyuantianshu_5", fenliejiesuan.get(name).get(0).get("zhuyuantianshu_3"));

            Map zje = new HashMap<>();
            zje.put("unit", "元");
            zje.put("unit", 0);
            if (m2.get(name).get("jin") != null)
                zje.put("value", zje);
            map.put("zongjine_9", zje);


            Map yzje = new HashMap<>();
            yzje.put("unit", "元");
            yzje.put("value", 0);
            if (m2.get(name).get("yjin") != null)
                zje.put("value", m2.get(name).get("yjin"));
            map.put("yibaofanweifeiyong_9", yzje);

            //明细
            Map<String, Object> CTMap = new HashMap<>();
            CTMap.put("unit", "元");
            CTMap.put("value", 0);
            Map<String, Object> CTcMap = new HashMap<>();
            CTcMap.put("unit", "次");
            CTcMap.put("value", 0);

            if (m1.get(name + "CT费") != null) {
                CTMap.put("value", m1.get(name + "CT费").get("value"));
                CTcMap.put("value", m1.get(name + "CT费").get("count"));
            }
            map.put("CTqiuhe_3", CTMap);
            map.put("CTjici_3", CTcMap);

            Map<String, Object> TCDMap = new HashMap<>();
            TCDMap.put("unit", "元");
            TCDMap.put("value", 0);
            Map<String, Object> TCDcMap = new HashMap<>();
            TCDcMap.put("unit", "次");
            TCDcMap.put("value", 0);

            if (m1.get(name + "TCD") != null)
                TCDMap.put("value", m1.get(name + "TCD").get("value"));
            map.put("TCDqiuhe_3", TCDMap);
            if (m1.get(name + "TCD") != null)
                TCDcMap.put("value", m1.get(name + "TCD").get("count"));
            map.put("TCDjici_3", TCDcMap);

            Map<String, Object> bingliMap = new HashMap<>();
            bingliMap.put("unit", "元");
            bingliMap.put("value", 0);
            Map<String, Object> bcMap = new HashMap<>();
            bcMap.put("unit", "次");
            bcMap.put("value", 0);

            if (m1.get(name + "病理费") != null)
                bingliMap.put("value", m1.get(name + "病理费").get("value"));
            map.put("bingliqiuhe_3", bingliMap);
            if (m1.get(name + "病理费") != null)
                bcMap.put("value", m1.get(name + "病理费").get("count"));
            map.put("binglijici_3", bcMap);

            Map<String, Object> cailiaoMap = new HashMap<>();
            cailiaoMap.put("unit", "元");
            cailiaoMap.put("value", 0);
            Map<String, Object> caicMap = new HashMap<>();
            caicMap.put("unit", "次");
            caicMap.put("value", 0);

            if (m1.get(name + "材料费") != null)
                cailiaoMap.put("value", m1.get(name + "材料费").get("value"));
            map.put("cailiaoqiuhe_3", cailiaoMap);
            if (m1.get(name + "材料费") != null)
                caicMap.put("value", m1.get(name + "材料费").get("count"));
            map.put("cailiaojici_3", caicMap);

            Map<String, Object> caichaoMap = new HashMap<>();
            caichaoMap.put("unit", "元");
            caichaoMap.put("value", 0);
            Map<String, Object> cailcMap = new HashMap<>();
            cailcMap.put("unit", "次");
            cailcMap.put("value", 0);

            if (m1.get(name + "彩超费") != null)
                caichaoMap.put("value", m1.get(name + "彩超费").get("value"));
            map.put("caichaoqiuhe_3", caichaoMap);
            if (m1.get(name + "彩超费") != null)
                cailcMap.put("value", m1.get(name + "彩超费").get("count"));
            map.put("caichaojici_3", cailcMap);


            Map<String, Object> caoyaoMap = new HashMap<>();
            caoyaoMap.put("unit", "元");
            caoyaoMap.put("value", 0);
            Map<String, Object> caocMap = new HashMap<>();
            caocMap.put("unit", "次");
            caocMap.put("value", 0);

            if (m1.get(name + "草药费") != null)
                caoyaoMap.put("value", m1.get(name + "草药费").get("value"));
            map.put("caoyaoqiuhe_3", caoyaoMap);
            if (m1.get(name + "草药费") != null)
                caocMap.put("value", m1.get(name + "草药费").get("count"));
            map.put("caoyaojici_3", caocMap);


            Map<String, Object> zhongchengMap = new HashMap<>();
            zhongchengMap.put("unit", "元");
            zhongchengMap.put("value", 0);
            Map<String, Object> zcMap = new HashMap<>();
            zcMap.put("unit", "次");
            zcMap.put("value", 0);


            if (m1.get(name + "中成药费") != null)
                zhongchengMap.put("value", m1.get(name + "中成药费").get("value"));
            map.put("zhongchengyaoqiuhe_3", zhongchengMap);
            if (m1.get(name + "中成药费") != null)
                zcMap.put("value", m1.get(name + "中成药费").get("count"));
            map.put("zhongchengyaojici_3", zcMap);


            Map<String, Object> ciMap = new HashMap<>();
            ciMap.put("unit", "元");
            ciMap.put("value", 0);
            Map<String, Object> cicMap = new HashMap<>();
            cicMap.put("unit", "次");
            cicMap.put("value", 0);

            if (m1.get(name + "磁共振") != null)
                ciMap.put("value", m1.get(name + "磁共振").get("value"));
            map.put("cigongzhenqiuhe_3", ciMap);
            if (m1.get(name + "磁共振") != null)
                cicMap.put("value", m1.get(name + "磁共振").get("count"));
            map.put("cigongzhenjici_3", cicMap);


            Map<String, Object> huMap = new HashMap<>();
            huMap.put("unit", "元");
            huMap.put("value", 0);
            Map<String, Object> hucMap = new HashMap<>();
            hucMap.put("unit", "次");
            hucMap.put("value", 0);

            if (m1.get(name + "护理费") != null)
                huMap.put("value", m1.get(name + "护理费").get("value"));
            map.put("huliqiuhe_3", huMap);
            if (m1.get(name + "护理费") != null)
                hucMap.put("value", m1.get(name + "护理费").get("count"));
            map.put("hulijici_3", hucMap);

            Map<String, Object> huaMap = new HashMap<>();
            huaMap.put("unit", "元");
            huaMap.put("value", 0);
            Map<String, Object> huacMap = new HashMap<>();
            huacMap.put("unit", "次");
            huacMap.put("value", 0);


            if (m1.get(name + "化验费") != null)
                huaMap.put("value", m1.get(name + "化验费").get("value"));
            map.put("huayanqiuhe_3", huaMap);
            if (m1.get(name + "化验费") != null)
                huacMap.put("value", m1.get(name + "化验费").get("count"));
            map.put("huayanjici_3", huacMap);

            Map<String, Object> huanMap = new HashMap<>();
            huanMap.put("unit", "元");
            huanMap.put("value", 0);
            Map<String, Object> huancMap = new HashMap<>();
            huancMap.put("unit", "次");
            huancMap.put("value", 0);

            if (m1.get(name + "换药费") != null)
                huanMap.put("value", m1.get(name + "换药费").get("value"));
            map.put("huanyaoqiuhe_3", huanMap);
            if (m1.get(name + "换药费") != null)
                huancMap.put("value", m1.get(name + "换药费").get("count"));
            map.put("huanyaojici_3", huancMap);

            Map<String, Object> jianMap = new HashMap<>();
            jianMap.put("unit", "元");
            jianMap.put("value", 0);
            Map<String, Object> jiancMap = new HashMap<>();
            jiancMap.put("unit", "次");
            jiancMap.put("value", 0);

            if (m1.get(name + "检查费") != null)
                jianMap.put("value", m1.get(name + "检查费").get("value"));
            map.put("jianchaqiuhe_3", jianMap);
            if (m1.get(name + "检查费") != null)
                jiancMap.put("value", m1.get(name + "检查费").get("count"));
            map.put("jianchajici_3", jiancMap);

            Map<String, Object> jiuMap = new HashMap<>();
            jiuMap.put("unit", "元");
            jiuMap.put("value", 0);
            Map<String, Object> jiucMap = new HashMap<>();
            jiucMap.put("unit", "次");
            jiucMap.put("value", 0);

            if (m1.get(name + "救护车") != null)
                jiuMap.put("value", m1.get(name + "救护车").get("value"));
            map.put("jiuhucheqiuhe_3", jiuMap);
            if (m1.get(name + "救护车") != null)
                jiucMap.put("value", m1.get(name + "救护车").get("count"));
            map.put("jiuhuchejici_3", jiucMap);

            Map<String, Object> liliaoMap = new HashMap<>();
            liliaoMap.put("unit", "元");
            liliaoMap.put("value", 0);
            Map<String, Object> liliaocMap = new HashMap<>();
            liliaocMap.put("unit", "次");
            liliaocMap.put("value", 0);

            if (m1.get(name + "理疗车") != null)
                liliaoMap.put("value", m1.get(name + "理疗费").get("value"));
            map.put("liliaoqiuhe_3", liliaoMap);
            if (m1.get(name + "理疗车") != null)
                liliaocMap.put("value", m1.get(name + "理疗费").get("count"));
            map.put("liliaojici_3", liliaocMap);

            Map<String, Object> maMap = new HashMap<>();
            maMap.put("unit", "元");
            maMap.put("value", 0);
            Map<String, Object> macMap = new HashMap<>();
            macMap.put("unit", "次");
            macMap.put("value", 0);

            if (m1.get(name + "麻醉费") != null)
                maMap.put("value", m1.get(name + "麻醉费").get("value"));
            map.put("mazuiqiuhe_3", maMap);
            if (m1.get(name + "麻醉费") != null)
                macMap.put("value", m1.get(name + "麻醉费").get("count"));
            map.put("mazuijici_3", macMap);

            Map<String, Object> naoMap = new HashMap<>();
            naoMap.put("unit", "元");
            naoMap.put("value", 0);
            Map<String, Object> naocMap = new HashMap<>();
            naocMap.put("unit", "次");
            naocMap.put("value", 0);

            if (m1.get(name + "脑电费") != null)
                naoMap.put("value", m1.get(name + "脑电费").get("value"));
            map.put("naodiantuqiuhe_3", naoMap);
            if (m1.get(name + "脑电费") != null)
                naocMap.put("value", m1.get(name + "脑电费").get("count"));
            map.put("naodiantujici_3", naocMap);

            Map<String, Object> paiMap = new HashMap<>();
            paiMap.put("unit", "元");
            paiMap.put("value", 0);
            Map<String, Object> paicMap = new HashMap<>();
            paicMap.put("unit", "次");
            paicMap.put("value", 0);

            if (m1.get(name + "拍片费") != null)
                paiMap.put("value", m1.get(name + "拍片费").get("value"));
            map.put("paipianqiuhe_3", paiMap);
            if (m1.get(name + "拍片费") != null)
                paicMap.put("value", m1.get(name + "拍片费").get("count"));
            map.put("paipianjici_3", paicMap);

            Map<String, Object> ssMap = new HashMap<>();
            ssMap.put("unit", "元");
            ssMap.put("value", 0);
            Map<String, Object> sscMap = new HashMap<>();
            sscMap.put("unit", "次");
            sscMap.put("value", 0);

            if (m1.get(name + "手术材料费") != null)
                ssMap.put("value", m1.get(name + "手术材料费").get("value"));
            map.put("shoushucailiaoqiuhe_3", ssMap);
            if (m1.get(name + "手术材料费") != null)
                sscMap.put("value", m1.get(name + "手术材料费").get("count"));
            map.put("shoushucailiaojici_3", sscMap);

            Map<String, Object> shouMap = new HashMap<>();
            shouMap.put("unit", "元");
            shouMap.put("value", 0);
            Map<String, Object> shoucMap = new HashMap<>();
            shoucMap.put("unit", "次");
            shoucMap.put("value", 0);


            if (m1.get(name + "手术费") != null)
                shouMap.put("value", m1.get(name + "手术费").get("value"));
            map.put("shoushuqiuhe_3", shouMap);
            if (m1.get(name + "手术费") != null)
                shoucMap.put("value", m1.get(name + "手术费").get("count"));
            map.put("shoushujici_3", shoucMap);

            Map<String, Object> tiMap = new HashMap<>();
            tiMap.put("unit", "元");
            tiMap.put("value", 0);
            Map<String, Object> ticMap = new HashMap<>();
            ticMap.put("unit", "次");
            ticMap.put("value", 0);

            if (m1.get(name + "体检费") != null)
                tiMap.put("value", m1.get(name + "体检费").get("value"));
            map.put("tijianqiuhe_3", tiMap);
            if (m1.get(name + "体检费") != null)
                ticMap.put("value", m1.get(name + "体检费").get("count"));
            map.put("tijianjici_3", ticMap);


            Map<String, Object> weiMap = new HashMap<>();
            weiMap.put("unit", "元");
            weiMap.put("value", 0);
            Map<String, Object> weicMap = new HashMap<>();
            weicMap.put("unit", "次");
            weicMap.put("value", 0);

            if (m1.get(name + "胃镜费") != null)
                weiMap.put("value", m1.get(name + "胃镜费").get("value"));
            map.put("weijingqiuhe_3", weiMap);
            if (m1.get(name + "胃镜费") != null)
                weicMap.put("value", m1.get(name + "胃镜费").get("count"));
            map.put("weijingjici_3", weicMap);

            Map<String, Object> xiMap = new HashMap<>();
            xiMap.put("unit", "元");
            xiMap.put("value", 0);
            Map<String, Object> xicMap = new HashMap<>();
            xicMap.put("unit", "次");
            xicMap.put("value", 0);

            if (m1.get(name + "西药费") != null)
                xiMap.put("value", m1.get(name + "西药费").get("value"));
            map.put("xiyaoqiuhe_3", xiMap);
            if (m1.get(name + "西药费") != null)
                xicMap.put("value", m1.get(name + "西药费").get("count"));
            map.put("xiyaojici_3", xicMap);

            Map<String, Object> xinMap = new HashMap<>();
            xinMap.put("unit", "元");
            xinMap.put("value", 0);
            Map<String, Object> xincMap = new HashMap<>();
            xincMap.put("unit", "次");
            xincMap.put("value", 0);

            if (m1.get(name + "心超费") != null)
                xinMap.put("value", m1.get(name + "心超费").get("value"));
            map.put("xinchaoqiuhe_3", xinMap);
            if (m1.get(name + "心超费") != null)
                xincMap.put("value", m1.get(name + "心超费").get("count"));
            map.put("xinchaojici_3", xincMap);

            Map<String, Object> zhenMap = new HashMap<>();
            zhenMap.put("unit", "元");
            zhenMap.put("value", 0);
            Map<String, Object> zhencMap = new HashMap<>();
            zhencMap.put("unit", "次");
            zhencMap.put("value", 0);

            if (m1.get(name + "诊疗费") != null)
                zhenMap.put("value", m1.get(name + "诊疗费").get("value"));
            map.put("zhenliaoqiuhe_3", zhenMap);
            if (m1.get(name + "诊疗费") != null)
                zhencMap.put("value", m1.get(name + "诊疗费").get("count"));
            map.put("zhenliaojici_3", zhencMap);

            Map<String, Object> zhiMap = new HashMap<>();
            zhiMap.put("unit", "元");
            zhiMap.put("value", 0);
            Map<String, Object> zhicMap = new HashMap<>();
            zhicMap.put("unit", "次");
            zhicMap.put("value", 0);

            if (m1.get(name + "治疗费") != null)
                zhiMap.put("value", m1.get(name + "治疗费").get("value"));
            map.put("zhiliaoqiuhe_3", zhiMap);
            if (m1.get(name + "治疗费") != null)
                zhicMap.put("value", m1.get(name + "治疗费").get("count"));
            map.put("zhiliaojici_3", zhicMap);

            Map<String, Object> zhuMap = new HashMap<>();
            zhuMap.put("unit", "元");
            zhuMap.put("value", 0);
            Map<String, Object> zhucMap = new HashMap<>();
            zhucMap.put("unit", "次");
            zhucMap.put("value", 0);

            if (m1.get(name + "注射费") != null)
                zhuMap.put("value", m1.get(name + "注射费").get("value"));
            map.put("zhusheqiuhe_3", zhuMap);
            if (m1.get(name + "注射费") != null)
                zhucMap.put("value", m1.get(name + "注射费").get("count"));
            map.put("zhushejici_3", zhucMap);


            //住院药品种类数量
            HashMap zyz = new HashMap();
            zyz.put("unit", "种");
            zyz.put("value", m3.get(name));
            map.put("zhuyuanyaopinzhongleishuliang_1", zyz);

            //诊疗项目金额
            if (map.get("zhenliaoqiuhe_3") != null)
                map.put("zhenliaoxiangmujine_1", map.get("zhenliaoqiuhe_3"));


            insertList.add(map);
            iii++;
            if (insertList.size() >= 1000) {
                mongoTemplate.insert(insertList, zhenliaoku);
                insertList = new ArrayList<>();
                System.out.println(iii);
            }

        }
        if (insertList.size() > 0) {
            mongoTemplate.insert(insertList, zhenliaoku);
            insertList = new ArrayList<>();
        }
        System.out.println("ok");
    }

    @Test
    void zhenliaoclear() {
        String zhenliaoku = "com.ns.entity.object.form.instance.nsjgpv79iu55j528ig78.zhenliaobiao_1";
        List<Map> all = mongoTemplate.findAll(Map.class, zhenliaoku);
        if (all.size() > 0) {
            mongoTemplate.remove(new Query(), zhenliaoku);
            // System.out.println(all);
        }
    }

    @Test
    void zhenliaodayclear() {
        String keshiku = "com.ns.entity.object.form.instance.ns3g475kl6jj2eb4ixfi.yibaozhenduanxinxi_1";
        List<Map> all = mongoTemplate.findAll(Map.class, keshiku);
        if (all.size() > 0) {
            mongoTemplate.remove(new Query(), keshiku);
            // System.out.println(all);
        }
    }

    @Test
    void keshibiaodaoru() {
        String keshiku = "com.ns.entity.object.form.instance.nsjgpv79iu55j528ig78.keshibiao_1";
        String yishengku = "com.ns.entity.object.form.instance.nsjgpv79iu55j528ig78.yishengbiao_1";
        String huanzheku = "com.ns.entity.object.form.instance.nsjgpv79iu55j528ig78.huanzhebiao_1";
        String jiesuanku = "com.ns.entity.object.form.instance.nsjgpv79iu55j528ig78.feiyongjiesuanxinxi_1";
        String mingxiku = "com.ns.entity.object.form.instance.nsjgpv79iu55j528ig78.feiyongmingxixinxi_1";
        String yaopinku = "com.ns.entity.object.form.instance.nsjgpv79iu55j528ig78.yaopinbiao_1";

        TypedAggregation<Map> TypedAggregation = Aggregation.newAggregation(
                Map.class,
                Aggregation.group("keshimingcheng_26")
        ).withOptions(Aggregation.newAggregationOptions().allowDiskUse(true).build());

        AggregationResults<Map> aggregationResults = mongoTemplate.aggregate(
                TypedAggregation,
                mingxiku,
                Map.class);
        List<Map> mappedResults = aggregationResults.getMappedResults();
        Set<String> kesnames = new HashSet<>();
        for (Map k : mappedResults) {
            kesnames.add(k.get("_id").toString());
        }

        int ii = 0;
        TypedAggregation<Map> TypedAggregation2 = Aggregation.newAggregation(
                Map.class,
                Aggregation.group("keshimingcheng_34")
                        .count().as("count")
        ).withOptions(Aggregation.newAggregationOptions().allowDiskUse(true).build());

        AggregationResults<Map> aggregationResults2 = mongoTemplate.aggregate(
                TypedAggregation2,
                huanzheku,
                Map.class);
        List<Map> mappedResults2 = aggregationResults2.getMappedResults();
        Map<String, Integer> krenshu = new HashMap<>();
        for (Map k : mappedResults2) {
            krenshu.put(k.get("_id").toString(), Integer.parseInt(k.get("count").toString()));
        }

        TypedAggregation<Map> TypedAggregation3 = Aggregation.newAggregation(
                Map.class,
                Aggregation.group("keshimingcheng_29")
                        .sum("jiliang_3.value").as("jil")
        ).withOptions(Aggregation.newAggregationOptions().allowDiskUse(true).build());

        AggregationResults<Map> aggregationResults3 = mongoTemplate.aggregate(
                TypedAggregation3,
                yaopinku,
                Map.class);
        List<Map> mappedResults3 = aggregationResults3.getMappedResults();
        Map<String, Double> yao = new HashMap<>();
        for (Map k : mappedResults3) {
            yao.put(k.get("_id").toString(), Double.parseDouble(k.get("jil").toString()));
        }

        TypedAggregation<Map> TypedAggregation4 = Aggregation.newAggregation(
                Map.class,
                Aggregation.group("ruyuankeshimingcheng_2")
                        .sum("yiliaofeizonge_6.value").as("jine")
                        .sum("tongchouzhifujine_5.value").as("yjine")
        ).withOptions(Aggregation.newAggregationOptions().allowDiskUse(true).build());

        AggregationResults<Map> aggregationResults4 = mongoTemplate.aggregate(
                TypedAggregation4,
                jiesuanku,
                Map.class);
        List<Map> mappedResults4 = aggregationResults4.getMappedResults();
        Map<String, Map> zyjine = new HashMap<>();
        for (Map k : mappedResults4) {
            zyjine.put(k.get("_id").toString(), k);
        }

        TypedAggregation<Map> TypedAggregation5 = Aggregation.newAggregation(
                Map.class,
                Aggregation.group("keshimingcheng_30")
                        .count().as("count")
        ).withOptions(Aggregation.newAggregationOptions().allowDiskUse(true).build());

        AggregationResults<Map> aggregationResults5 = mongoTemplate.aggregate(
                TypedAggregation5,
                yishengku,
                Map.class);
        List<Map> mappedResults5 = aggregationResults5.getMappedResults();

        Map<String, Integer> m5 = new HashMap<>();
        for (Map k : mappedResults5) {
            m5.put(k.get("_id").toString(), Integer.parseInt(k.get("count").toString()));
        }

        TypedAggregation<Map> TypedAggregation6 = Aggregation.newAggregation(
                Map.class,
                Aggregation.group("ruyuankeshimingcheng_2", "yiliaoleibie_8")
                        .count().as("count")
        ).withOptions(Aggregation.newAggregationOptions().allowDiskUse(true).build());

        AggregationResults<Map> aggregationResults6 = mongoTemplate.aggregate(
                TypedAggregation6,
                jiesuanku,
                Map.class);
        List<Map> mappedResults6 = aggregationResults6.getMappedResults();

        Map<String, Map> m6 = new HashMap<>();
        for (Map<String, Map> k : mappedResults6) {
            m6.put(k.get("_id").get("ruyuankeshimingcheng_2").toString()
                            + k.get("_id").get("yiliaoleibie_8").toString()
                    , k);
        }

        TypedAggregation<Map> TypedAggregation7 = Aggregation.newAggregation(
                Map.class,
                Aggregation.group("keshimingcheng_26", "shoufeixiangmuleibie_5").count().as("count"));
        AggregationResults<Map> aggregationResults7 = mongoTemplate.aggregate(
                TypedAggregation7,
                mingxiku,
                Map.class);
        List<Map> mappedResults7 = aggregationResults7.getMappedResults();

        Map<String, Map> m7 = new HashMap<>();
        for (Map<String, Map> k : mappedResults7) {
            if (k.get("_id").get("shoufeixiangmuleibie_5").equals("手术费")) {
                m7.put(k.get("_id").get("keshimingcheng_26").toString(), k);
            }
        }

        Query q1 = new Query();
        q1.fields().include("xingming_103").include("chushengriqi_28")
                .include("nianling_23").include("xingbie_22")
                .include("xianzhongleixing_2").include("danweimingcheng_25")
                .include("ruyuankeshimingcheng_2").include("zhuzhiyishengdaima_1")
                .include("zhuzhiyisheng_1").include("ruyuanjibingmingcheng_3")
                .include("chuyuanjibingmingcheng_3").include("ruyuanriqi_2")
                .include("chuyuanriqi_2").include("zhuyuantianshu_3")
                .include("danjuhao_12").include("chuyuankeshimingcheng_2")
                .exclude("_id");

        List<Map> jiehuanzhe = mongoTemplate.find(q1, Map.class, jiesuanku);

        Map<String, List<Map>> fenliejiesuan = new HashMap<>();
        for (Map k : jiehuanzhe) {
            String fieldValue = k.get("ruyuankeshimingcheng_2").toString(); // 假设用于分组的字段名为 "fieldName"
            if (fenliejiesuan.get(fieldValue) == null) {
                fenliejiesuan.put(fieldValue, new ArrayList<>());
            }
            fenliejiesuan.get(fieldValue).add(k);
        }
        jiehuanzhe.clear();


        for (String name : kesnames) {
            if (zyjine.get(name) == null)
                continue;
            Map<String, Object> map = new HashMap<>();

            //系统字段
            String mainId = UUID.randomUUID().toString();
            map.put("_id", mainId);
            map.put("create_time", System.currentTimeMillis());
            map.put("create_account", "admin");
            map.put("category_id", "985655af-649a-4c0e-8a85-52073533f246");
            map.put("data_status", "已归档");
            map.put("data_type", 1);
            map.put("priority", "");
            map.put("bind_id", mainId);
            map.put("corp_id", "nsrcu88p7uy22m7i9ioz");
            map.put("parent_corp_id_list", new ArrayList<>());
            map.put("bind_category_id", "");

            //业务字段（结算）
            map.put("yiyuanmingcheng_14", "孝感市第一人民医院");
            map.put("yiyuanleibie_12", "公立医院");
            map.put("yiyuandengji_12", "三级");
            map.put("yiyuanxingzhi_12", "");
            map.put("keshimingcheng_36", name);
            map.put("yibaoniandu_14", 2023);

            Map ren = new HashMap<>();
            ren.put("unit", "个");
            ren.put("value", 0);
            if (krenshu.get(name) != null)
                ren.put("value", krenshu.get(name));
            map.put("huanzherenshu_2", ren);

            Map zjil = new HashMap<>();
            zjil.put("unit", "个");
            zjil.put("value", 0);
            if (yao.get(name) != null)
                zjil.put("value", yao.get(name));
            map.put("jiliang_4", zjil);

            Map zjine = new HashMap<>();
            zjine.put("unit", "元");
            zjine.put("value", 0);
            if (zyjine.get(name).get("jine") != null)
                zjine.put("value", zyjine.get(name).get("jine"));
            map.put("jine_23", zjine);

            map.put("danweishijianzongjine_5", zjine);
            Map yzjine = new HashMap<>();
            yzjine.put("unit", "元");
            yzjine.put("value", 0);
            if (zyjine.get(name).get("yjine") != null)
                yzjine.put("value", zyjine.get(name).get("yjine"));
            map.put("danweishijianyibaobaoxiaozonge_5", yzjine);

            Map yzb = new HashMap<>();
            yzb.put("unit", "比");
            yzb.put("value", 0);
            if (zyjine.get(name).get("jine") != null && zyjine.get(name).get("yjine") != null) {
                Object j = Double.parseDouble(zyjine.get(name).get("yjine").toString()) / Double.parseDouble(zyjine.get(name).get("jine").toString());
                yzb.put("value", j);
            }
            map.put("danweishijianyibaobili_5", yzb);

            Map yishengshu = new HashMap<>();
            yishengshu.put("unit", "个");
            yishengshu.put("value", 0);
            if (m5.get(name) != null)
                yishengshu.put("value", m5.get(name));
            map.put("danweishijianyishengshuliang_3", yishengshu);


            int s1 = 0, s2 = 0;
            Map zhu = new HashMap<>();
            zhu.put("unit", "个");
            zhu.put("value", 0);
            if (m6.get(name + "住院") != null) {
                zhu.put("value", m6.get(name + "住院").get("count"));
                s1 = Integer.parseInt(m6.get(name + "住院").get("count").toString());
            }

            map.put("danweishijianzhuyuanrenci_3", zhu);

            Map ssr = new HashMap<>();
            ssr.put("unit", "个");
            ssr.put("value", s1);
            if (m6.get(name + "普通门诊") != null) {
                s2 = Integer.parseInt(m6.get(name + "普通门诊").get("count").toString());
                s1 += s2;
                ssr.put("value", s1);
            }
            map.put("danweishijianneijiezhenrenci_3", ssr);

            Map dss = new HashMap<>();
            dss.put("unit", "个");
            dss.put("value", 0);
            if (m7.get(name) != null)
                dss.put("value", m7.get(name).get("count"));

            map.put("danweishijianshoushurenci_3", dss);

            ii++;
            mongoTemplate.insert(map, keshiku);
            System.out.println(ii);
        }
    }

    @Test
    void doctorday() {
        String jiesuanku = "com.ns.entity.object.form.instance.nsjgpv79iu55j528ig78.feiyongjiesuanxinxi_1";
        String mingxiku = "com.ns.entity.object.form.instance.nsjgpv79iu55j528ig78.feiyongmingxixinxi_1";
        String doctordayku = "com.ns.entity.object.form.instance.nsjgpv79iu55j528ig78.yishengbiaoanri_1";

        int iii = 0;
        Query q1 = new Query();
        q1.fields()
                .include("ruyuankeshimingcheng_2").include("zhuzhiyishengdaima_1")
                .include("zhuzhiyisheng_1").include("feiyongjiesuanshijian_5")
                .include("chuyuankeshimingcheng_2")
                .include("yiliaoleibie_8").exclude("_id");

        List<Map> jiedoctor = mongoTemplate.find(q1, Map.class, jiesuanku);

        Map<String, List<Map>> fenliejiesuan = new HashMap<>();
        for (Map k : jiedoctor) {
            String fieldValue = k.get("zhuzhiyishengdaima_1").toString() + k.get("feiyongjiesuanshijian_5").toString(); // 假设用于分组的字段名为 "fieldName"
            if (fenliejiesuan.get(fieldValue) == null) {
                fenliejiesuan.put(fieldValue, new ArrayList<>());
            }
            fenliejiesuan.get(fieldValue).add(k);
        }
        jiedoctor.clear();

        TypedAggregation<Map> TypedAggregation = Aggregation.newAggregation(
                Map.class,
                Aggregation.group("zhuzhiyishengdaima_1", "feiyongjiesuanshijian_5")
                        .sum("yiliaofeizonge_6.value").as("money")
                        .sum("tongchouzhifujine_5.value").as("ybmoney")
        ).withOptions(Aggregation.newAggregationOptions().allowDiskUse(true).build());

        AggregationResults<Map> aggregationResults = mongoTemplate.aggregate(
                TypedAggregation,
                jiesuanku,
                Map.class);
        List<Map> mappedResults = aggregationResults.getMappedResults();

        Map<String, Map> encodeTime = new HashMap<>();
        for (Map<String, Map> k : mappedResults) {
            encodeTime.put(k.get("_id").get("zhuzhiyishengdaima_1").toString()
                    + k.get("_id").get("feiyongjiesuanshijian_5").toString(), k);
        }

        TypedAggregation<Map> TypedAggregation2 = Aggregation.newAggregation(
                Map.class,
                Aggregation.group("yishengbianma_5", "shoufeixiangmuleibie_5", "feiyongjiesuanshijian_6")
                        .sum("jine_20.value").as("value")
                        .count().as("count")
        ).withOptions(Aggregation.newAggregationOptions().allowDiskUse(true).build());
        ;

        AggregationResults<Map> aggregationResults2 = mongoTemplate.aggregate(
                TypedAggregation2,
                mingxiku,
                Map.class);
        List<Map> mappedResults2 = aggregationResults2.getMappedResults();

        Map<String, Map> summingTimes = new HashMap<>();
        for (Map<String, Map> k : mappedResults2) {
            summingTimes.put(k.get("_id").get("yishengbianma_5").toString()
                    + k.get("_id").get("feiyongjiesuanshijian_6").toString()
                    + k.get("_id").get("shoufeixiangmuleibie_5").toString(), k);
        }


        TypedAggregation<Map> TypedAggregation3 = Aggregation.newAggregation(
                Map.class,
                Aggregation.group("zhuzhiyishengdaima_1", "feiyongjiesuanshijian_5")
                        .addToSet("xingming_103").as("set")
        ).withOptions(Aggregation.newAggregationOptions().allowDiskUse(true).build());

        AggregationResults<Map> aggregationResults3 = mongoTemplate.aggregate(
                TypedAggregation3,
                jiesuanku,
                Map.class);
        List<Map> mappedResults3 = aggregationResults3.getMappedResults();


        Map<String, Map> mm = new HashMap<>();
        for (Map<String, Map> k : mappedResults3) {
            mm.put(k.get("_id").get("zhuzhiyishengdaima_1").toString()
                    + k.get("_id").get("feiyongjiesuanshijian_5"), k);
        }


        TypedAggregation<Map> TypedAggregation4 = Aggregation.newAggregation(
                Map.class,
                Aggregation.group("zhuzhiyishengdaima_1", "feiyongjiesuanshijian_5", "yiliaoleibie_8")
                        .count().as("count")
        ).withOptions(Aggregation.newAggregationOptions().allowDiskUse(true).build());
        AggregationResults<Map> aggregationResults4 = mongoTemplate.aggregate(
                TypedAggregation4,
                jiesuanku,
                Map.class);
        List<Map> mappedResults4 = aggregationResults4.getMappedResults();

        Map<String, Map> m4 = new HashMap<>();
        for (Map<String, Map> k : mappedResults4) {
            m4.put(k.get("_id").get("zhuzhiyishengdaima_1").toString()
                    + k.get("_id").get("feiyongjiesuanshijian_5"), k);
        }

        List insert = new ArrayList<>();

        for (String et : encodeTime.keySet()) {
            if (fenliejiesuan.get(et) == null) {
                continue;
            }
            Map<String, Object> map = new HashMap<>();
            //系统字段
            String mainId = UUID.randomUUID().toString();
            map.put("_id", mainId);
            map.put("create_time", System.currentTimeMillis());
            map.put("create_account", "admin");
            map.put("category_id", "a4026211-cbdd-46c9-8a8c-72bec2b2725e");
            map.put("data_status", "已归档");
            map.put("data_type", 1);
            map.put("priority", "");
            map.put("bind_id", mainId);
            map.put("corp_id", "nsrcu88p7uy22m7i9ioz");
            map.put("parent_corp_id_list", new ArrayList<>());
            map.put("bind_category_id", "");

            //业务字段
            map.put("yiyuanmingcheng_9", "孝感市第一人民医院");
            if (fenliejiesuan.get(et).get(0).get("ruyuankeshimingcheng_2") != null)
                map.put("keshimingcheng_31", fenliejiesuan.get(et).get(0).get("ruyuankeshimingcheng_2"));
            if (fenliejiesuan.get(et).get(0).get("chuyuankeshimingcheng_2") != null)
                map.put("keshimingcheng_31", fenliejiesuan.get(et).get(0).get("chuyuankeshimingcheng_2"));
            if (fenliejiesuan.get(et).get(0).get("zhuzhiyisheng_1") != null)
                map.put("yishengmingcheng_2", fenliejiesuan.get(et).get(0).get("zhuzhiyisheng_1"));


            map.put("yibaoniandu_9", 2023);
            map.put("yiyuanleibie_7", "公立医院");
            map.put("yiyuandengji_7", "三级");
            map.put("yiyuanxingzhi_7", "");
            if (fenliejiesuan.get(et).get(0).get("feiyongjiesuanshijian_5") != null) {
                map.put("dangrishijian_1", fenliejiesuan.get(et).get(0).get("feiyongjiesuanshijian_5"));
                map.put("fashengriqi_6", fenliejiesuan.get(et).get(0).get("feiyongjiesuanshijian_5"));
            }


            Map daymoney = new HashMap<>();
            Double zm = 0.0;
            daymoney.put("unit", "元");
            daymoney.put("value", 0);
            if (encodeTime.get(et).get("money") != null) {
                daymoney.put("value", encodeTime.get(et).get("money"));
                zm = Double.parseDouble(encodeTime.get(et).get("money").toString());
            }
            map.put("rijiezhenzongjine_2", daymoney);
            map.put("danweishijianzongjine_2", daymoney);

            Map ybdaymoney = new HashMap<>();
            ybdaymoney.put("unit", "元");
            ybdaymoney.put("value", 0);
            Double yzm = 0.0;
            if (encodeTime.get(et).get("ybmoney") != null) {
                ybdaymoney.put("value", encodeTime.get(et).get("ybmoney"));
                yzm = Double.parseDouble(encodeTime.get(et).get("ybmoney").toString());
            }
            map.put("yibaofanweifeiyong_8", ybdaymoney);
            map.put("danweishijianyibaobaoxiaozonge_2", ybdaymoney);

            Map bi = new HashMap<>();
            bi.put("unit", "比");
            bi.put("value", 0);
            if (encodeTime.get(et).get("ybmoney") != null && encodeTime.get(et).get("money") != null)
                bi.put("value", yzm / zm);

            map.put("danweishijianyibaobili_2", bi);

            ArrayList<String> jiancha = new ArrayList<>();


            //检查检验
            Map jmoney = new HashMap<>();
            jmoney.put("unit", "元");
            Double lj = 0.0;
            Integer lc = 0;
            Map ccount = new HashMap<>();
            ccount.put("unit", "次");

//            jiancha.add("CT费");
//            jiancha.add("TCD");
//            jiancha.add("彩超费");
//            jiancha.add("磁共振");
//            jiancha.add("化验费");
//            jiancha.add("检查费");
//            jiancha.add("脑电图");
//            jiancha.add("拍片费");
//            jiancha.add("胃镜费");
//            jiancha.add("心超费");

            if (summingTimes.get(et + "CT费") != null) {
                lj += Double.parseDouble(summingTimes.get(et + "CT费").get("value").toString());
                lc += Integer.parseInt(summingTimes.get(et + "CT费").get("count").toString());
            }
            if (summingTimes.get(et + "TCD") != null) {
                lj += Double.parseDouble(summingTimes.get(et + "TCD").get("value").toString());
                lc += Integer.parseInt(summingTimes.get(et + "TCD").get("count").toString());
            }
            if (summingTimes.get(et + "彩超费") != null) {
                lj += Double.parseDouble(summingTimes.get(et + "彩超费").get("value").toString());
                lc += Integer.parseInt(summingTimes.get(et + "彩超费").get("count").toString());
            }
            if (summingTimes.get(et + "磁共振") != null) {
                lj += Double.parseDouble(summingTimes.get(et + "磁共振").get("value").toString());
                lc += Integer.parseInt(summingTimes.get(et + "磁共振").get("count").toString());
            }
            if (summingTimes.get(et + "化验费") != null) {
                lj += Double.parseDouble(summingTimes.get(et + "化验费").get("value").toString());
                lc += Integer.parseInt(summingTimes.get(et + "化验费").get("count").toString());
            }
            if (summingTimes.get(et + "检查费") != null) {
                lj += Double.parseDouble(summingTimes.get(et + "检查费").get("value").toString());
                lc += Integer.parseInt(summingTimes.get(et + "检查费").get("count").toString());
            }
            if (summingTimes.get(et + "脑电图") != null) {
                lj += Double.parseDouble(summingTimes.get(et + "脑电图").get("value").toString());
                lc += Integer.parseInt(summingTimes.get(et + "脑电图").get("count").toString());
            }
            if (summingTimes.get(et + "拍片费") != null) {
                lj += Double.parseDouble(summingTimes.get(et + "拍片费").get("value").toString());
                lc += Integer.parseInt(summingTimes.get(et + "拍片费").get("count").toString());
            }
            if (summingTimes.get(et + "胃镜费") != null) {
                lj += Double.parseDouble(summingTimes.get(et + "胃镜费").get("value").toString());
                lc += Integer.parseInt(summingTimes.get(et + "胃镜费").get("count").toString());
            }
            if (summingTimes.get(et + "心超费") != null) {
                lj += Double.parseDouble(summingTimes.get(et + "心超费").get("value").toString());
                lc += Integer.parseInt(summingTimes.get(et + "心超费").get("count").toString());
            }

            jmoney.put("value", lj);
            ccount.put("value", lc);

            map.put("jianchajianyanqiuhe_2", jmoney);
            map.put("jianchajianyanjici_2", ccount);

            //日接诊开药种类
            Map yaol = new HashMap<>();
            yaol.put("unit", "种");
            Integer lei = 0;
            if (summingTimes.get(et + "中成药费") != null)
                lei++;
            if (summingTimes.get(et + "西药费") != null)
                lei++;
            if (summingTimes.get(et + "草药费") != null)
                lei++;

            yaol.put("value", lei);

            map.put("rijiezhenkaiyaozhonglei_2", yaol);

            //日接诊次数
            Map dayjz = new HashMap<>();
            dayjz.put("unit", "次");
            dayjz.put("value", 0);
            Integer r = 0;
            if (mm.get(et) != null) {
                List x = (List) mm.get(et).get("set");
                dayjz.put("value", x.size());
                r = x.size();
            }
            map.put("rijiezhencishu_2", dayjz);
            map.put("danweishijianneijiezhenrenci_2", dayjz);


            Map yjc = new HashMap<>();
            yjc.put("unit", "次");
            yjc.put("value", 0);
            Integer ll = 1;
            //医疗类别计次
            if (r == 1) {
                yjc.put("value", 1);
            } else if (r > 1) {
                for (int i = 1; i < fenliejiesuan.get(et).size(); i++) {
                    if (fenliejiesuan.get(et).get(0).get("yiliaoleibie_8") != fenliejiesuan.get(et).get(i).get("yiliaoleibie_8"))
                        ll = 2;
                }
            }
            if (r == 0) {
                ll = 0;
            }
            yjc.put("value", ll);
            map.put("yiliaoleibiejici_2", yjc);

            //日接诊人均金额
            Map dayr = new HashMap<>();
            dayr.put("unit", "元");
            dayr.put("value", 0);
            Double dm = 0.0;
            if (encodeTime.get(et).get("money") != null)
                dm = Double.parseDouble(encodeTime.get(et).get("money").toString()) / r;
            dayr.put("value", dm);
            map.put("rijiezhenrenjunjine_2", dayr);


            //单位时间住院人次
            Map dayzc = new HashMap<>();
            dayzc.put("unit", "次");
            dayzc.put("value", 0);
            if (m4.get(et + "住院") != null) {
                dayzc.put("value", m4.get(et + "住院").size());
            }
            map.put("danweishijianzhuyuanrenci_2", dayzc);

            //CT求和
            Map<String, Object> CTMap = new HashMap<>();
            CTMap.put("unit", "元");
            CTMap.put("value", 0);
            Map<String, Object> CTcMap = new HashMap<>();
            CTcMap.put("unit", "次");
            CTcMap.put("value", 0);

            if (summingTimes.get(et + "CT费") != null) {
                CTMap.put("value", summingTimes.get(et + "CT费").get("value"));
                CTcMap.put("value", summingTimes.get(et + "CT费").get("count"));
            }
            map.put("CTqiuhe_2", CTMap);
            map.put("CTjici_2", CTcMap);

            Map<String, Object> TCDMap = new HashMap<>();
            TCDMap.put("unit", "元");
            TCDMap.put("value", 0);
            Map<String, Object> TCDcMap = new HashMap<>();
            TCDcMap.put("unit", "次");
            TCDcMap.put("value", 0);

            if (summingTimes.get(et + "TCD") != null)
                TCDMap.put("value", summingTimes.get(et + "TCD").get("value"));
            map.put("TCDqiuhe_2", TCDMap);
            if (summingTimes.get(et + "TCD") != null)
                TCDcMap.put("value", summingTimes.get(et + "TCD").get("count"));
            map.put("TCDjici_2", TCDcMap);

            Map<String, Object> bingliMap = new HashMap<>();
            bingliMap.put("unit", "元");
            bingliMap.put("value", 0);
            Map<String, Object> bcMap = new HashMap<>();
            bcMap.put("unit", "次");
            bcMap.put("value", 0);

            if (summingTimes.get(et + "病理费") != null)
                bingliMap.put("value", summingTimes.get(et + "病理费").get("value"));
            map.put("bingliqiuhe_2", bingliMap);
            if (summingTimes.get(et + "病理费") != null)
                bcMap.put("value", summingTimes.get(et + "病理费").get("count"));
            map.put("binglijici_2", bcMap);

            Map<String, Object> cailiaoMap = new HashMap<>();
            cailiaoMap.put("unit", "元");
            cailiaoMap.put("value", 0);
            Map<String, Object> caicMap = new HashMap<>();
            caicMap.put("unit", "次");
            caicMap.put("value", 0);

            if (summingTimes.get(et + "材料费") != null)
                cailiaoMap.put("value", summingTimes.get(et + "材料费").get("value"));
            map.put("cailiaoqiuhe_2", cailiaoMap);
            if (summingTimes.get(et + "材料费") != null)
                caicMap.put("value", summingTimes.get(et + "材料费").get("count"));
            map.put("cailiaojici_2", caicMap);

            Map<String, Object> caichaoMap = new HashMap<>();
            caichaoMap.put("unit", "元");
            caichaoMap.put("value", 0);
            Map<String, Object> cailcMap = new HashMap<>();
            cailcMap.put("unit", "次");
            cailcMap.put("value", 0);

            if (summingTimes.get(et + "彩超费") != null)
                caichaoMap.put("value", summingTimes.get(et + "彩超费").get("value"));
            map.put("caichaoqiuhe_2", caichaoMap);
            if (summingTimes.get(et + "彩超费") != null)
                cailcMap.put("value", summingTimes.get(et + "彩超费").get("count"));
            map.put("caichaojici_2", cailcMap);


            Map<String, Object> caoyaoMap = new HashMap<>();
            caoyaoMap.put("unit", "元");
            caoyaoMap.put("value", 0);
            Map<String, Object> caocMap = new HashMap<>();
            caocMap.put("unit", "次");
            caocMap.put("value", 0);

            if (summingTimes.get(et + "草药费") != null)
                caoyaoMap.put("value", summingTimes.get(et + "草药费").get("value"));
            map.put("caoyaoqiuhe_2", caoyaoMap);
            if (summingTimes.get(et + "草药费") != null)
                caocMap.put("value", summingTimes.get(et + "草药费").get("count"));
            map.put("caoyaojici_2", caocMap);


            Map<String, Object> zhongchengMap = new HashMap<>();
            zhongchengMap.put("unit", "元");
            zhongchengMap.put("value", 0);
            Map<String, Object> zcMap = new HashMap<>();
            zcMap.put("unit", "次");
            zcMap.put("value", 0);


            if (summingTimes.get(et + "中成药费") != null)
                zhongchengMap.put("value", summingTimes.get(et + "中成药费").get("value"));
            map.put("zhongchengyaoqiuhe_2", zhongchengMap);
            if (summingTimes.get(et + "中成药费") != null)
                zcMap.put("value", summingTimes.get(et + "中成药费").get("count"));
            map.put("zhongchengyaojici_2", zcMap);


            Map<String, Object> ciMap = new HashMap<>();
            ciMap.put("unit", "元");
            ciMap.put("value", 0);
            Map<String, Object> cicMap = new HashMap<>();
            cicMap.put("unit", "次");
            cicMap.put("value", 0);

            if (summingTimes.get(et + "磁共振") != null)
                ciMap.put("value", summingTimes.get(et + "磁共振").get("value"));
            map.put("cigongzhenqiuhe_2", ciMap);
            if (summingTimes.get(et + "磁共振") != null)
                cicMap.put("value", summingTimes.get(et + "磁共振").get("count"));
            map.put("cigongzhenjici_2", cicMap);


            Map<String, Object> huMap = new HashMap<>();
            huMap.put("unit", "元");
            huMap.put("value", 0);
            Map<String, Object> hucMap = new HashMap<>();
            hucMap.put("unit", "次");
            hucMap.put("value", 0);

            if (summingTimes.get(et + "护理费") != null)
                huMap.put("value", summingTimes.get(et + "护理费").get("value"));
            map.put("huliqiuhe_2", huMap);
            if (summingTimes.get(et + "护理费") != null)
                hucMap.put("value", summingTimes.get(et + "护理费").get("count"));
            map.put("hulijici_2", hucMap);

            Map<String, Object> huaMap = new HashMap<>();
            huaMap.put("unit", "元");
            huaMap.put("value", 0);
            Map<String, Object> huacMap = new HashMap<>();
            huacMap.put("unit", "次");
            huacMap.put("value", 0);


            if (summingTimes.get(et + "化验费") != null)
                huaMap.put("value", summingTimes.get(et + "化验费").get("value"));
            map.put("huayanqiuhe_2", huaMap);
            if (summingTimes.get(et + "化验费") != null)
                huacMap.put("value", summingTimes.get(et + "化验费").get("count"));
            map.put("huayanjici_2", huacMap);

            Map<String, Object> huanMap = new HashMap<>();
            huanMap.put("unit", "元");
            huanMap.put("value", 0);
            Map<String, Object> huancMap = new HashMap<>();
            huancMap.put("unit", "次");
            huancMap.put("value", 0);

            if (summingTimes.get(et + "换药费") != null)
                huanMap.put("value", summingTimes.get(et + "换药费").get("value"));
            map.put("huanyaoqiuhe_2", huanMap);
            if (summingTimes.get(et + "换药费") != null)
                huancMap.put("value", summingTimes.get(et + "换药费").get("count"));
            map.put("huanyaojici_2", huancMap);

            Map<String, Object> jianMap = new HashMap<>();
            jianMap.put("unit", "元");
            jianMap.put("value", 0);
            Map<String, Object> jiancMap = new HashMap<>();
            jiancMap.put("unit", "次");
            jiancMap.put("value", 0);

            if (summingTimes.get(et + "检查费") != null)
                jianMap.put("value", summingTimes.get(et + "检查费").get("value"));
            map.put("jianchaqiuhe_2", jianMap);
            if (summingTimes.get(et + "检查费") != null)
                jiancMap.put("value", summingTimes.get(et + "检查费").get("count"));
            map.put("jianchajici_2", jiancMap);

            Map<String, Object> jiuMap = new HashMap<>();
            jiuMap.put("unit", "元");
            jiuMap.put("value", 0);
            Map<String, Object> jiucMap = new HashMap<>();
            jiucMap.put("unit", "次");
            jiucMap.put("value", 0);

            if (summingTimes.get(et + "救护车") != null)
                jiuMap.put("value", summingTimes.get(et + "救护车").get("value"));
            map.put("jiuhucheqiuhe_2", jiuMap);
            if (summingTimes.get(et + "救护车") != null)
                jiucMap.put("value", summingTimes.get(et + "救护车").get("count"));
            map.put("jiuhuchejici_2", jiucMap);

            Map<String, Object> liliaoMap = new HashMap<>();
            liliaoMap.put("unit", "元");
            liliaoMap.put("value", 0);
            Map<String, Object> liliaocMap = new HashMap<>();
            liliaocMap.put("unit", "次");
            liliaocMap.put("value", 0);

            if (summingTimes.get(et + "理疗车") != null)
                liliaoMap.put("value", summingTimes.get(et + "理疗费").get("value"));
            map.put("liliaoqiuhe_2", liliaoMap);
            if (summingTimes.get(et + "理疗车") != null)
                liliaocMap.put("value", summingTimes.get(et + "理疗费").get("count"));
            map.put("liliaojici_2", liliaocMap);

            Map<String, Object> maMap = new HashMap<>();
            maMap.put("unit", "元");
            maMap.put("value", 0);
            Map<String, Object> macMap = new HashMap<>();
            macMap.put("unit", "次");
            macMap.put("value", 0);

            if (summingTimes.get(et + "麻醉费") != null)
                maMap.put("value", summingTimes.get(et + "麻醉费").get("value"));
            map.put("mazuiqiuhe_2", maMap);
            if (summingTimes.get(et + "麻醉费") != null)
                macMap.put("value", summingTimes.get(et + "麻醉费").get("count"));
            map.put("mazuijici_2", macMap);

            Map<String, Object> naoMap = new HashMap<>();
            naoMap.put("unit", "元");
            naoMap.put("value", 0);
            Map<String, Object> naocMap = new HashMap<>();
            naocMap.put("unit", "次");
            naocMap.put("value", 0);

            if (summingTimes.get(et + "脑电费") != null)
                naoMap.put("value", summingTimes.get(et + "脑电费").get("value"));
            map.put("naodiantuqiuhe_2", naoMap);
            if (summingTimes.get(et + "脑电费") != null)
                naocMap.put("value", summingTimes.get(et + "脑电费").get("count"));
            map.put("naodiantujici_2", naocMap);

            Map<String, Object> paiMap = new HashMap<>();
            paiMap.put("unit", "元");
            paiMap.put("value", 0);
            Map<String, Object> paicMap = new HashMap<>();
            paicMap.put("unit", "次");
            paicMap.put("value", 0);

            if (summingTimes.get(et + "拍片费") != null)
                paiMap.put("value", summingTimes.get(et + "拍片费").get("value"));
            map.put("paipianqiuhe_2", paiMap);
            if (summingTimes.get(et + "拍片费") != null)
                paicMap.put("value", summingTimes.get(et + "拍片费").get("count"));
            map.put("paipianjici_2", paicMap);

            Map<String, Object> ssMap = new HashMap<>();
            ssMap.put("unit", "元");
            ssMap.put("value", 0);
            Map<String, Object> sscMap = new HashMap<>();
            sscMap.put("unit", "次");
            sscMap.put("value", 0);

            if (summingTimes.get(et + "手术材料费") != null)
                ssMap.put("value", summingTimes.get(et + "手术材料费").get("value"));
            map.put("shoushucailiaoqiuhe_2", ssMap);
            if (summingTimes.get(et + "手术材料费") != null)
                sscMap.put("value", summingTimes.get(et + "手术材料费").get("count"));
            map.put("shoushucailiaojici_2", sscMap);

            Map<String, Object> shouMap = new HashMap<>();
            shouMap.put("unit", "元");
            shouMap.put("value", 0);
            Map<String, Object> shoucMap = new HashMap<>();
            shoucMap.put("unit", "次");
            shoucMap.put("value", 0);


            if (summingTimes.get(et + "手术费") != null)
                shouMap.put("value", summingTimes.get(et + "手术费").get("value"));
            map.put("shoushuqiuhe_2", shouMap);
            if (summingTimes.get(et + "手术费") != null)
                shoucMap.put("value", summingTimes.get(et + "手术费").get("count"));
            map.put("shoushujici_2", shoucMap);
            map.put("danweishijianshoushurenci_2", shoucMap);

            Map<String, Object> tiMap = new HashMap<>();
            tiMap.put("unit", "元");
            tiMap.put("value", 0);
            Map<String, Object> ticMap = new HashMap<>();
            ticMap.put("unit", "次");
            ticMap.put("value", 0);

            if (summingTimes.get(et + "体检费") != null)
                tiMap.put("value", summingTimes.get(et + "体检费").get("value"));
            map.put("tijianqiuhe_2", tiMap);
            if (summingTimes.get(et + "体检费") != null)
                ticMap.put("value", summingTimes.get(et + "体检费").get("count"));
            map.put("tijianjici_2", ticMap);


            Map<String, Object> weiMap = new HashMap<>();
            weiMap.put("unit", "元");
            weiMap.put("value", 0);
            Map<String, Object> weicMap = new HashMap<>();
            weicMap.put("unit", "次");
            weicMap.put("value", 0);

            if (summingTimes.get(et + "胃镜费") != null)
                weiMap.put("value", summingTimes.get(et + "胃镜费").get("value"));
            map.put("weijingqiuhe_2", weiMap);
            if (summingTimes.get(et + "胃镜费") != null)
                weicMap.put("value", summingTimes.get(et + "胃镜费").get("count"));
            map.put("weijingjici_2", weicMap);

            Map<String, Object> xiMap = new HashMap<>();
            xiMap.put("unit", "元");
            xiMap.put("value", 0);
            Map<String, Object> xicMap = new HashMap<>();
            xicMap.put("unit", "次");
            xicMap.put("value", 0);

            if (summingTimes.get(et + "西药费") != null)
                xiMap.put("value", summingTimes.get(et + "西药费").get("value"));
            map.put("xiyaoqiuhe_2", xiMap);
            if (summingTimes.get(et + "西药费") != null)
                xicMap.put("value", summingTimes.get(et + "西药费").get("count"));
            map.put("xiyaojici_2", xicMap);

            Map<String, Object> xinMap = new HashMap<>();
            xinMap.put("unit", "元");
            xinMap.put("value", 0);
            Map<String, Object> xincMap = new HashMap<>();
            xincMap.put("unit", "次");
            xincMap.put("value", 0);

            if (summingTimes.get(et + "心超费") != null)
                xinMap.put("value", summingTimes.get(et + "心超费").get("value"));
            map.put("xinchaoqiuhe_2", xinMap);
            if (summingTimes.get(et + "心超费") != null)
                xincMap.put("value", summingTimes.get(et + "心超费").get("count"));
            map.put("xinchaojici_2", xincMap);

            Map<String, Object> zhenMap = new HashMap<>();
            zhenMap.put("unit", "元");
            zhenMap.put("value", 0);
            Map<String, Object> zhencMap = new HashMap<>();
            zhencMap.put("unit", "次");
            zhencMap.put("value", 0);

            if (summingTimes.get(et + "诊疗费") != null)
                zhenMap.put("value", summingTimes.get(et + "诊疗费").get("value"));
            map.put("zhenliaoqiuhe_2", zhenMap);
            if (summingTimes.get(et + "诊疗费") != null)
                zhencMap.put("value", summingTimes.get(et + "诊疗费").get("count"));
            map.put("zhenliaojici_2", zhencMap);

            Map<String, Object> zhiMap = new HashMap<>();
            zhiMap.put("unit", "元");
            zhiMap.put("value", 0);
            Map<String, Object> zhicMap = new HashMap<>();
            zhicMap.put("unit", "次");
            zhicMap.put("value", 0);

            if (summingTimes.get(et + "治疗费") != null)
                zhiMap.put("value", summingTimes.get(et + "治疗费").get("value"));
            map.put("zhiliaoqiuhe_2", zhiMap);
            if (summingTimes.get(et + "治疗费") != null)
                zhicMap.put("value", summingTimes.get(et + "治疗费").get("count"));
            map.put("zhiliaojici_2", zhicMap);

            Map<String, Object> zhuMap = new HashMap<>();
            zhuMap.put("unit", "元");
            zhuMap.put("value", 0);
            Map<String, Object> zhucMap = new HashMap<>();
            zhucMap.put("unit", "次");
            zhucMap.put("value", 0);

            if (summingTimes.get(et + "注射费") != null)
                zhuMap.put("value", summingTimes.get(et + "注射费").get("value"));
            map.put("zhusheqiuhe_2", zhuMap);
            if (summingTimes.get(et + "注射费") != null)
                zhucMap.put("value", summingTimes.get(et + "注射费").get("count"));
            map.put("zhushejici_2", zhucMap);
            iii++;
            insert.add(map);
            if (insert.size() >= 500) {
                mongoTemplate.insert(insert, doctordayku);
                insert = new ArrayList<>();
                System.out.println(iii);
            }

        }
        if (insert.size() > 0) {
            mongoTemplate.insert(insert, doctordayku);
            insert = new ArrayList<>();
        }
    }

    @Test
    void patientday() {
        String jiesuanku = "com.ns.entity.object.form.instance.nsjgpv79iu55j528ig78.feiyongjiesuanxinxi_1";
        String mingxiku = "com.ns.entity.object.form.instance.nsjgpv79iu55j528ig78.feiyongmingxixinxi_1";
        String huanzhedayku = "com.ns.entity.object.form.instance.nsjgpv79iu55j528ig78.huanzhebiaoanri_1";

        int iii = 0;
        Query q1 = new Query();
        q1.fields()
                .include("ruyuankeshimingcheng_2").include("zhuzhiyishengdaima_1")
                .include("zhuzhiyisheng_1").include("feiyongjiesuanshijian_5")
                .include("chuyuankeshimingcheng_2").include("xingming_103")
                .include("chushengriqi_28").include("nianling_23")
                .include("xingbie_22").include("danjuhao_12")
                .include("danweimingcheng_25").include("ruyuanjibingmingcheng_3")
                .include("chuyuanjibingmingcheng_3").include("zhuyuantianshu_3")
                .include("yiliaoleibie_8").exclude("_id");

        List<Map> jiepat = mongoTemplate.find(q1, Map.class, jiesuanku);
        Map<String, List<Map>> fenliejiesuan = new HashMap<>();
        for (Map k : jiepat) {
            String fieldValue = k.get("xingming_103").toString() + k.get("feiyongjiesuanshijian_5").toString(); // 假设用于分组的字段名为 "fieldName"
            if (fenliejiesuan.get(fieldValue) == null) {
                fenliejiesuan.put(fieldValue, new ArrayList<>());
            }
            fenliejiesuan.get(fieldValue).add(k);
        }
        jiepat.clear();

        TypedAggregation<Map> TypedAggregation = Aggregation.newAggregation(
                Map.class,
                Aggregation.group("xingming_103", "feiyongjiesuanshijian_5")
                        .sum("yiliaofeizonge_6.value").as("money")
                        .sum("tongchouzhifujine_5.value").as("ybmoney")
        ).withOptions(Aggregation.newAggregationOptions().allowDiskUse(true).build());

        AggregationResults<Map> aggregationResults = mongoTemplate.aggregate(
                TypedAggregation,
                jiesuanku,
                Map.class);
        List<Map> mappedResults = aggregationResults.getMappedResults();

        Map<String, Map> encodeTime = new HashMap<>();
        for (Map<String, Map> k : mappedResults) {
            encodeTime.put(k.get("_id").get("xingming_103").toString()
                    + k.get("_id").get("feiyongjiesuanshijian_5").toString(), k);
        }


        TypedAggregation<Map> TypedAggregation2 = Aggregation.newAggregation(
                Map.class,
                Aggregation.group("xingming_104", "shoufeixiangmuleibie_5", "feiyongjiesuanshijian_6")
                        .sum("jine_20.value").as("value")
                        .count().as("count")
        ).withOptions(Aggregation.newAggregationOptions().allowDiskUse(true).build());
        ;

        AggregationResults<Map> aggregationResults2 = mongoTemplate.aggregate(
                TypedAggregation2,
                mingxiku,
                Map.class);
        List<Map> mappedResults2 = aggregationResults2.getMappedResults();
        Map<String, Map> summingTimes = new HashMap<>();
        for (Map<String, Map> k : mappedResults2) {
            summingTimes.put(k.get("_id").get("xingming_104").toString()
                    + k.get("_id").get("feiyongjiesuanshijian_6").toString()
                    + k.get("_id").get("shoufeixiangmuleibie_5").toString(), k);
        }

        TypedAggregation<Map> TypedAggregation3 = Aggregation.newAggregation(
                Map.class,
                Aggregation.group("xingming_103", "feiyongjiesuanshijian_5")
                        .addToSet("zhuzhiyishengdaima_1").as("set")
        ).withOptions(Aggregation.newAggregationOptions().allowDiskUse(true).build());

        AggregationResults<Map> aggregationResults3 = mongoTemplate.aggregate(
                TypedAggregation3,
                jiesuanku,
                Map.class);
        List<Map> mappedResults3 = aggregationResults3.getMappedResults();

        Map<String, Map> m3 = new HashMap<>();
        for (Map<String, Map> k : mappedResults3) {
            m3.put(k.get("_id").get("xingming_103").toString()
                    + k.get("_id").get("feiyongjiesuanshijian_5"), k);
        }

        List insert = new ArrayList<>();

        for (String et : encodeTime.keySet()) {
            if (fenliejiesuan.get(et) == null) {
                continue;
            }
            Map<String, Object> map = new HashMap<>();
            //系统字段
            String mainId = UUID.randomUUID().toString();
            map.put("_id", mainId);
            map.put("create_time", System.currentTimeMillis());
            map.put("create_account", "admin");
            map.put("category_id", "52e7ff29-b665-4dc2-af11-e0869cb46552");
            map.put("data_status", "已归档");
            map.put("data_type", 1);
            map.put("priority", "");
            map.put("bind_id", mainId);
            map.put("corp_id", "nsrcu88p7uy22m7i9ioz");
            map.put("parent_corp_id_list", new ArrayList<>());
            map.put("bind_category_id", "");

            map.put("zhengjianhaoma_29", "");
            map.put("shehuibaozhangkahao_12", "");
            map.put("nianling_28", fenliejiesuan.get(et).get(0).get("nianling_23"));
            map.put("xingming_110", fenliejiesuan.get(et).get(0).get("xingming_103"));
            map.put("xingbie_27", fenliejiesuan.get(et).get(0).get("xingbie_22"));
            if (fenliejiesuan.get(et).get(0).get("chushengriqi_28") != null)
                map.put("chushengriqi_33", fenliejiesuan.get(et).get(0).get("chushengriqi_28"));
            if (fenliejiesuan.get(et).get(0).get("danweimingcheng_25") != null) {
                map.put("huanzhegongzuodanwei_2", fenliejiesuan.get(et).get(0).get("danweimingcheng_25"));
                map.put("diqujiedaoxiangzhen_2", fenliejiesuan.get(et).get(0).get("danweimingcheng_25"));
            }

            map.put("yibaoniandu_13", 2023);
            map.put("xianzhongleixing_9", "医保");
            if (fenliejiesuan.get(et).get(0).get("yiliaoleibie_8") != null)
                map.put("yiliaoleibie_17", fenliejiesuan.get(et).get(0).get("yiliaoleibie_8"));

            map.put("yiyuanmingcheng_13", "孝感市第一人民医院");
            map.put("yiyuanleibie_11", "公立医院");
            map.put("yiyuandengji_11", "三级");

            if (fenliejiesuan.get(et).get(0).get("ruyuankeshimingcheng_2") != null) {
                map.put("keshimingcheng_35", fenliejiesuan.get(et).get(0).get("ruyuankeshimingcheng_2"));
                map.put("zhixingkeshimingcheng_15", fenliejiesuan.get(et).get(0).get("ruyuankeshimingcheng_2"));
            }

            if (fenliejiesuan.get(et).get(0).get("chuyuankeshimingcheng_2") != null) {
                map.put("keshimingcheng_35", fenliejiesuan.get(et).get(0).get("chuyuankeshimingcheng_2"));
                map.put("zhixingkeshimingcheng_15", fenliejiesuan.get(et).get(0).get("chuyuankeshimingcheng_2"));
            }
            if (fenliejiesuan.get(et).get(0).get("ruyuanjibingmingcheng_3") != null) {
                map.put("zhuzhenjibingmingcheng_8", fenliejiesuan.get(et).get(0).get("ruyuanjibingmingcheng_3"));
            }
            if (fenliejiesuan.get(et).get(0).get("chuyuanjibingmingcheng_3") != null) {
                map.put("zhuzhenjibingmingcheng_8", fenliejiesuan.get(et).get(0).get("chuyuanjibingmingcheng_3"));
            }

            Map zje = new HashMap<>();
            zje.put("unit", "元");
            zje.put("value", 0);
            Map yje = new HashMap<>();
            yje.put("unit", "元");
            yje.put("value", 0);
            Map bje = new HashMap<>();
            bje.put("unit", "比");
            bje.put("value", 0);
            Double z = 0.0, y = 0.0, b = 0.0;
            if (encodeTime.get(et).get("money") != null) {
                z = Double.parseDouble(encodeTime.get(et).get("money").toString());
                zje.put("value", encodeTime.get(et).get("money"));
            }
            map.put("zongjine_12", zje);
            map.put("danweishijianzongjine_4", zje);
            if (encodeTime.get(et).get("ybmoney") != null) {
                y = Double.parseDouble(encodeTime.get(et).get("ybmoney").toString());
                yje.put("value", encodeTime.get(et).get("ybmoney"));
            }
            map.put("yibaofanweifeiyong_12", yje);
            map.put("danweishijianyibaobaoxiaozonge_4", yje);
            if (encodeTime.get(et).get("money") != null
                    && encodeTime.get(et).get("ybmoney") != null) {
                b = y / z;
                bje.put("value", b);
            }
            map.put("danweishijianyibaobili_4", bje);

            map.put("shifouzhuyuan_2", "否");
            if (fenliejiesuan.get(et).get(0).get("yiliaoleibie_8") != null
                    && fenliejiesuan.get(et).get(0).get("yiliaoleibie_8").toString()
                    .equals("住院")) {
                map.put("shifouzhuyuan_2", "是");
            }

            Map bmc = new HashMap<>();
            bmc.put("unit", "次");
            bmc.put("value", 1);
            if (m3.get(et) != null) {
                bmc.put("value", m3.get(et).size());
            }

            map.put("yishengbianmajicishu_2", bmc);
            if (fenliejiesuan.get(et).get(0).get("zhuzhiyisheng_1") != null) {
                map.put("yishengxingming_8", fenliejiesuan.get(et).get(0).get("zhuzhiyisheng_1"));
            }

            Map jzcs = new HashMap<>();
            jzcs.put("unit", "个");
            jzcs.put("value", 1);

            if (fenliejiesuan.get(et).get(0).get("ruyuankeshimingcheng_2") != null
                    && fenliejiesuan.get(et).get(0).get("chuyuankeshimingcheng_2") != null
                    && fenliejiesuan.get(et).get(0).get("ruyuankeshimingcheng_2")
                    != fenliejiesuan.get(et).get(0).get("chuyuankeshimingcheng_2")
            ) {
                jzcs.put("value", 2);
            }
            if (fenliejiesuan.get(et).get(0).get("zhuyuantianshu_3") != null)
                map.put("zhuyuantianshu_4", fenliejiesuan.get(et).get(0).get("zhuyuantianshu_3"));
            map.put("jiuzhenkeshishuliang_2", jzcs);
            Map zy = new HashMap<>();
            zy.put("unit", "次");
            zy.put("value", 0);

            map.put("zhuyuancishushu_2", zy);
            jzcs.put("unit", "次");
            if (fenliejiesuan.get(et).get(0).get("yiliaoleibie_8") != null
                    && fenliejiesuan.get(et).get(0).get("yiliaoleibie_8").toString()
                    .equals("住院")) {
                map.put("zhuyuancishushu_2", jzcs);
            }

            Map hbzl = new HashMap<>();
            hbzl.put("unit", "种");
            hbzl.put("value", 1);
            if (fenliejiesuan.get(et).get(0).get("ruyuanjibingmingcheng_3") != null
                    && fenliejiesuan.get(et).get(0).get("chuyuanjibingmingcheng_3") != null
                    && fenliejiesuan.get(et).get(0).get("ruyuanjibingmingcheng_3")
                    != fenliejiesuan.get(et).get(0).get("chuyuanjibingmingcheng_3")
            ) {
                hbzl.put("value", 2);
            }
            map.put("huanbingzhonglei_2", hbzl);
            //检查检验
            Map jmoney = new HashMap<>();
            jmoney.put("unit", "元");
            Double lj = 0.0;
            Integer lc = 0;
            Map ccount = new HashMap<>();
            ccount.put("unit", "次");


            if (summingTimes.get(et + "CT费") != null) {
                lj += Double.parseDouble(summingTimes.get(et + "CT费").get("value").toString());
                lc += Integer.parseInt(summingTimes.get(et + "CT费").get("count").toString());
            }
            if (summingTimes.get(et + "TCD") != null) {
                lj += Double.parseDouble(summingTimes.get(et + "TCD").get("value").toString());
                lc += Integer.parseInt(summingTimes.get(et + "TCD").get("count").toString());
            }
            if (summingTimes.get(et + "彩超费") != null) {
                lj += Double.parseDouble(summingTimes.get(et + "彩超费").get("value").toString());
                lc += Integer.parseInt(summingTimes.get(et + "彩超费").get("count").toString());
            }
            if (summingTimes.get(et + "磁共振") != null) {
                lj += Double.parseDouble(summingTimes.get(et + "磁共振").get("value").toString());
                lc += Integer.parseInt(summingTimes.get(et + "磁共振").get("count").toString());
            }
            if (summingTimes.get(et + "化验费") != null) {
                lj += Double.parseDouble(summingTimes.get(et + "化验费").get("value").toString());
                lc += Integer.parseInt(summingTimes.get(et + "化验费").get("count").toString());
            }
            if (summingTimes.get(et + "检查费") != null) {
                lj += Double.parseDouble(summingTimes.get(et + "检查费").get("value").toString());
                lc += Integer.parseInt(summingTimes.get(et + "检查费").get("count").toString());
            }
            if (summingTimes.get(et + "脑电图") != null) {
                lj += Double.parseDouble(summingTimes.get(et + "脑电图").get("value").toString());
                lc += Integer.parseInt(summingTimes.get(et + "脑电图").get("count").toString());
            }
            if (summingTimes.get(et + "拍片费") != null) {
                lj += Double.parseDouble(summingTimes.get(et + "拍片费").get("value").toString());
                lc += Integer.parseInt(summingTimes.get(et + "拍片费").get("count").toString());
            }
            if (summingTimes.get(et + "胃镜费") != null) {
                lj += Double.parseDouble(summingTimes.get(et + "胃镜费").get("value").toString());
                lc += Integer.parseInt(summingTimes.get(et + "胃镜费").get("count").toString());
            }
            if (summingTimes.get(et + "心超费") != null) {
                lj += Double.parseDouble(summingTimes.get(et + "心超费").get("value").toString());
                lc += Integer.parseInt(summingTimes.get(et + "心超费").get("count").toString());
            }

            jmoney.put("value", lj);
            ccount.put("value", lc);

            map.put("menzhenjianchajine_2", jmoney);
            map.put("menzhenjianchaxiangmushuliang_2", ccount);


            Map nianl = new HashMap<>();
            nianl = (Map) map.get("nianling_28");
            if ((Integer) nianl.get("value") < 0) {
                map.put("nianlingzu_2", "非法");
            }
            if ((Integer) nianl.get("value") <= 6) {
                map.put("nianlingzu_2", "婴幼儿");
            } else if ((Integer) nianl.get("value") <= 12) {
                map.put("nianlingzu_2", "少儿");
            } else if ((Integer) nianl.get("value") <= 17) {
                map.put("nianlingzu_2", "青少年");
            } else if ((Integer) nianl.get("value") <= 45) {
                map.put("nianlingzu_2", "青年");
            } else if ((Integer) nianl.get("value") <= 69) {
                map.put("nianlingzu_2", "中年");
            } else if ((Integer) nianl.get("value") > 69) {
                map.put("nianlingzu_2", "老年");
            } else {
                map.put("nianlingzu_2", "非法");
            }

            map.put("fashengriqi_7", fenliejiesuan.get(et).get(0).get("feiyongjiesuanshijian_5"));

            //CT求和
            Map<String, Object> CTMap = new HashMap<>();
            CTMap.put("unit", "元");
            CTMap.put("value", 0);
            Map<String, Object> CTcMap = new HashMap<>();
            CTcMap.put("unit", "次");
            CTcMap.put("value", 0);

            if (summingTimes.get(et + "CT费") != null) {
                CTMap.put("value", summingTimes.get(et + "CT费").get("value"));
                CTcMap.put("value", summingTimes.get(et + "CT费").get("count"));
            }
            map.put("CTqiuhe_6", CTMap);
            map.put("CTjicishu_2", CTcMap);

            Map<String, Object> TCDMap = new HashMap<>();
            TCDMap.put("unit", "元");
            TCDMap.put("value", 0);
            Map<String, Object> TCDcMap = new HashMap<>();
            TCDcMap.put("unit", "次");
            TCDcMap.put("value", 0);

            if (summingTimes.get(et + "TCD") != null)
                TCDMap.put("value", summingTimes.get(et + "TCD").get("value"));
            map.put("TCDqiuhe_6", TCDMap);
            if (summingTimes.get(et + "TCD") != null)
                TCDcMap.put("value", summingTimes.get(et + "TCD").get("count"));
            map.put("TCDjicishu_2", TCDcMap);

            Map<String, Object> bingliMap = new HashMap<>();
            bingliMap.put("unit", "元");
            bingliMap.put("value", 0);
            Map<String, Object> bcMap = new HashMap<>();
            bcMap.put("unit", "次");
            bcMap.put("value", 0);

            if (summingTimes.get(et + "病理费") != null)
                bingliMap.put("value", summingTimes.get(et + "病理费").get("value"));
            map.put("bingliqiuhe_6", bingliMap);
            if (summingTimes.get(et + "病理费") != null)
                bcMap.put("value", summingTimes.get(et + "病理费").get("count"));
            map.put("binglijicishu_2", bcMap);

            Map<String, Object> cailiaoMap = new HashMap<>();
            cailiaoMap.put("unit", "元");
            cailiaoMap.put("value", 0);
            Map<String, Object> caicMap = new HashMap<>();
            caicMap.put("unit", "次");
            caicMap.put("value", 0);

            if (summingTimes.get(et + "材料费") != null)
                cailiaoMap.put("value", summingTimes.get(et + "材料费").get("value"));
            map.put("cailiaoqiuhe_6", cailiaoMap);
            if (summingTimes.get(et + "材料费") != null)
                caicMap.put("value", summingTimes.get(et + "材料费").get("count"));
            map.put("cailiaojicishu_2", caicMap);

            Map<String, Object> caichaoMap = new HashMap<>();
            caichaoMap.put("unit", "元");
            caichaoMap.put("value", 0);
            Map<String, Object> cailcMap = new HashMap<>();
            cailcMap.put("unit", "次");
            cailcMap.put("value", 0);

            if (summingTimes.get(et + "彩超费") != null)
                caichaoMap.put("value", summingTimes.get(et + "彩超费").get("value"));
            map.put("caichaoqiuhe_6", caichaoMap);
            if (summingTimes.get(et + "彩超费") != null)
                cailcMap.put("value", summingTimes.get(et + "彩超费").get("count"));
            map.put("caichaojicishu_2", cailcMap);


            Map<String, Object> caoyaoMap = new HashMap<>();
            caoyaoMap.put("unit", "元");
            caoyaoMap.put("value", 0);
            Map<String, Object> caocMap = new HashMap<>();
            caocMap.put("unit", "次");
            caocMap.put("value", 0);

            if (summingTimes.get(et + "草药费") != null)
                caoyaoMap.put("value", summingTimes.get(et + "草药费").get("value"));
            map.put("caoyaoqiuhe_6", caoyaoMap);
            if (summingTimes.get(et + "草药费") != null)
                caocMap.put("value", summingTimes.get(et + "草药费").get("count"));
            map.put("caoyaojicishu_2", caocMap);


            Map<String, Object> zhongchengMap = new HashMap<>();
            zhongchengMap.put("unit", "元");
            zhongchengMap.put("value", 0);
            Map<String, Object> zcMap = new HashMap<>();
            zcMap.put("unit", "次");
            zcMap.put("value", 0);


            if (summingTimes.get(et + "中成药费") != null)
                zhongchengMap.put("value", summingTimes.get(et + "中成药费").get("value"));
            map.put("zhongchengyaoqiuhe_6", zhongchengMap);
            if (summingTimes.get(et + "中成药费") != null)
                zcMap.put("value", summingTimes.get(et + "中成药费").get("count"));
            map.put("zhongchengyaojicishu_2", zcMap);


            Map<String, Object> ciMap = new HashMap<>();
            ciMap.put("unit", "元");
            ciMap.put("value", 0);
            Map<String, Object> cicMap = new HashMap<>();
            cicMap.put("unit", "次");
            cicMap.put("value", 0);

            if (summingTimes.get(et + "磁共振") != null)
                ciMap.put("value", summingTimes.get(et + "磁共振").get("value"));
            map.put("cigongzhenqiuhe_6", ciMap);
            if (summingTimes.get(et + "磁共振") != null)
                cicMap.put("value", summingTimes.get(et + "磁共振").get("count"));
            map.put("cigongzhenjicishu_2", cicMap);


            Map<String, Object> huMap = new HashMap<>();
            huMap.put("unit", "元");
            huMap.put("value", 0);
            Map<String, Object> hucMap = new HashMap<>();
            hucMap.put("unit", "次");
            hucMap.put("value", 0);

            if (summingTimes.get(et + "护理费") != null)
                huMap.put("value", summingTimes.get(et + "护理费").get("value"));
            map.put("huliqiuhe_6", huMap);
            if (summingTimes.get(et + "护理费") != null)
                hucMap.put("value", summingTimes.get(et + "护理费").get("count"));
            map.put("hulijicishu_2", hucMap);

            Map<String, Object> huaMap = new HashMap<>();
            huaMap.put("unit", "元");
            huaMap.put("value", 0);
            Map<String, Object> huacMap = new HashMap<>();
            huacMap.put("unit", "次");
            huacMap.put("value", 0);


            if (summingTimes.get(et + "化验费") != null)
                huaMap.put("value", summingTimes.get(et + "化验费").get("value"));
            map.put("huayanqiuhe_6", huaMap);
            if (summingTimes.get(et + "化验费") != null)
                huacMap.put("value", summingTimes.get(et + "化验费").get("count"));
            map.put("huayanjicishu_2", huacMap);

            Map<String, Object> huanMap = new HashMap<>();
            huanMap.put("unit", "元");
            huanMap.put("value", 0);
            Map<String, Object> huancMap = new HashMap<>();
            huancMap.put("unit", "次");
            huancMap.put("value", 0);

            if (summingTimes.get(et + "换药费") != null)
                huanMap.put("value", summingTimes.get(et + "换药费").get("value"));
            map.put("huanyaoqiuhe_6", huanMap);
            if (summingTimes.get(et + "换药费") != null)
                huancMap.put("value", summingTimes.get(et + "换药费").get("count"));
            map.put("huanyaojicishu_2", huancMap);

            Map<String, Object> jianMap = new HashMap<>();
            jianMap.put("unit", "元");
            jianMap.put("value", 0);
            Map<String, Object> jiancMap = new HashMap<>();
            jiancMap.put("unit", "次");
            jiancMap.put("value", 0);

            if (summingTimes.get(et + "检查费") != null)
                jianMap.put("value", summingTimes.get(et + "检查费").get("value"));
            map.put("jianchaqiuhe_6", jianMap);
            if (summingTimes.get(et + "检查费") != null)
                jiancMap.put("value", summingTimes.get(et + "检查费").get("count"));
            map.put("jianchajicishu_2", jiancMap);

            Map<String, Object> jiuMap = new HashMap<>();
            jiuMap.put("unit", "元");
            jiuMap.put("value", 0);
            Map<String, Object> jiucMap = new HashMap<>();
            jiucMap.put("unit", "次");
            jiucMap.put("value", 0);

            if (summingTimes.get(et + "救护车") != null)
                jiuMap.put("value", summingTimes.get(et + "救护车").get("value"));
            map.put("jiuhucheqiuhe_6", jiuMap);
            if (summingTimes.get(et + "救护车") != null)
                jiucMap.put("value", summingTimes.get(et + "救护车").get("count"));
            map.put("jiuhuchejicishu_2", jiucMap);

            Map<String, Object> liliaoMap = new HashMap<>();
            liliaoMap.put("unit", "元");
            liliaoMap.put("value", 0);
            Map<String, Object> liliaocMap = new HashMap<>();
            liliaocMap.put("unit", "次");
            liliaocMap.put("value", 0);

            if (summingTimes.get(et + "理疗车") != null)
                liliaoMap.put("value", summingTimes.get(et + "理疗费").get("value"));
            map.put("liliaoqiuhe_6", liliaoMap);
            if (summingTimes.get(et + "理疗车") != null)
                liliaocMap.put("value", summingTimes.get(et + "理疗费").get("count"));
            map.put("liliaojicishu_2", liliaocMap);

            Map<String, Object> maMap = new HashMap<>();
            maMap.put("unit", "元");
            maMap.put("value", 0);
            Map<String, Object> macMap = new HashMap<>();
            macMap.put("unit", "次");
            macMap.put("value", 0);

            if (summingTimes.get(et + "麻醉费") != null)
                maMap.put("value", summingTimes.get(et + "麻醉费").get("value"));
            map.put("mazuiqiuhe_6", maMap);
            if (summingTimes.get(et + "麻醉费") != null)
                macMap.put("value", summingTimes.get(et + "麻醉费").get("count"));
            map.put("mazuijicishu_2", macMap);

            Map<String, Object> naoMap = new HashMap<>();
            naoMap.put("unit", "元");
            naoMap.put("value", 0);
            Map<String, Object> naocMap = new HashMap<>();
            naocMap.put("unit", "次");
            naocMap.put("value", 0);

            if (summingTimes.get(et + "脑电费") != null)
                naoMap.put("value", summingTimes.get(et + "脑电费").get("value"));
            map.put("naodiantuqiuhe_6", naoMap);
            if (summingTimes.get(et + "脑电费") != null)
                naocMap.put("value", summingTimes.get(et + "脑电费").get("count"));
            map.put("naodiantujicishu_2", naocMap);

            Map<String, Object> paiMap = new HashMap<>();
            paiMap.put("unit", "元");
            paiMap.put("value", 0);
            Map<String, Object> paicMap = new HashMap<>();
            paicMap.put("unit", "次");
            paicMap.put("value", 0);

            if (summingTimes.get(et + "拍片费") != null)
                paiMap.put("value", summingTimes.get(et + "拍片费").get("value"));
            map.put("paipianqiuhe_6", paiMap);
            if (summingTimes.get(et + "拍片费") != null)
                paicMap.put("value", summingTimes.get(et + "拍片费").get("count"));
            map.put("paipianjicishu_2", paicMap);

            Map<String, Object> ssMap = new HashMap<>();
            ssMap.put("unit", "元");
            ssMap.put("value", 0);
            Map<String, Object> sscMap = new HashMap<>();
            sscMap.put("unit", "次");
            sscMap.put("value", 0);

            if (summingTimes.get(et + "手术材料费") != null)
                ssMap.put("value", summingTimes.get(et + "手术材料费").get("value"));
            map.put("shoushucailiaoqiuhe_6", ssMap);
            if (summingTimes.get(et + "手术材料费") != null)
                sscMap.put("value", summingTimes.get(et + "手术材料费").get("count"));
            map.put("shoushucailiaojicishu_2", sscMap);

            Map<String, Object> shouMap = new HashMap<>();
            shouMap.put("unit", "元");
            shouMap.put("value", 0);
            Map<String, Object> shoucMap = new HashMap<>();
            shoucMap.put("unit", "次");
            shoucMap.put("value", 0);


            if (summingTimes.get(et + "手术费") != null)
                shouMap.put("value", summingTimes.get(et + "手术费").get("value"));
            map.put("shoushuqiuhe_6", shouMap);
            if (summingTimes.get(et + "手术费") != null)
                shoucMap.put("value", summingTimes.get(et + "手术费").get("count"));
            map.put("shoushujicishu_2", shoucMap);

            Map<String, Object> tiMap = new HashMap<>();
            tiMap.put("unit", "元");
            tiMap.put("value", 0);
            Map<String, Object> ticMap = new HashMap<>();
            ticMap.put("unit", "次");
            ticMap.put("value", 0);

            if (summingTimes.get(et + "体检费") != null)
                tiMap.put("value", summingTimes.get(et + "体检费").get("value"));
            map.put("tijianqiuhe_6", tiMap);
            if (summingTimes.get(et + "体检费") != null)
                ticMap.put("value", summingTimes.get(et + "体检费").get("count"));
            map.put("tijianjicishu_2", ticMap);


            Map<String, Object> weiMap = new HashMap<>();
            weiMap.put("unit", "元");
            weiMap.put("value", 0);
            Map<String, Object> weicMap = new HashMap<>();
            weicMap.put("unit", "次");
            weicMap.put("value", 0);

            if (summingTimes.get(et + "胃镜费") != null)
                weiMap.put("value", summingTimes.get(et + "胃镜费").get("value"));
            map.put("weijingqiuhe_6", weiMap);
            if (summingTimes.get(et + "胃镜费") != null)
                weicMap.put("value", summingTimes.get(et + "胃镜费").get("count"));
            map.put("weijingjicishu_2", weicMap);

            Map<String, Object> xiMap = new HashMap<>();
            xiMap.put("unit", "元");
            xiMap.put("value", 0);
            Map<String, Object> xicMap = new HashMap<>();
            xicMap.put("unit", "次");
            xicMap.put("value", 0);

            if (summingTimes.get(et + "西药费") != null)
                xiMap.put("value", summingTimes.get(et + "西药费").get("value"));
            map.put("xiyaoqiuhe_6", xiMap);
            if (summingTimes.get(et + "西药费") != null)
                xicMap.put("value", summingTimes.get(et + "西药费").get("count"));
            map.put("xiyaojicishu_2", xicMap);

            Map<String, Object> xinMap = new HashMap<>();
            xinMap.put("unit", "元");
            xinMap.put("value", 0);
            Map<String, Object> xincMap = new HashMap<>();
            xincMap.put("unit", "次");
            xincMap.put("value", 0);

            if (summingTimes.get(et + "心超费") != null)
                xinMap.put("value", summingTimes.get(et + "心超费").get("value"));
            map.put("xinchaoqiuhe_6", xinMap);
            if (summingTimes.get(et + "心超费") != null)
                xincMap.put("value", summingTimes.get(et + "心超费").get("count"));
            map.put("xinchaojicishu_2", xincMap);

            Map<String, Object> zhenMap = new HashMap<>();
            zhenMap.put("unit", "元");
            zhenMap.put("value", 0);
            Map<String, Object> zhencMap = new HashMap<>();
            zhencMap.put("unit", "次");
            zhencMap.put("value", 0);

            if (summingTimes.get(et + "诊疗费") != null)
                zhenMap.put("value", summingTimes.get(et + "诊疗费").get("value"));
            map.put("zhenliaoqiuhe_6", zhenMap);
            if (summingTimes.get(et + "诊疗费") != null)
                zhencMap.put("value", summingTimes.get(et + "诊疗费").get("count"));
            map.put("zhenliaojicishu_2", zhencMap);

            Map<String, Object> zhiMap = new HashMap<>();
            zhiMap.put("unit", "元");
            zhiMap.put("value", 0);
            Map<String, Object> zhicMap = new HashMap<>();
            zhicMap.put("unit", "次");
            zhicMap.put("value", 0);

            if (summingTimes.get(et + "治疗费") != null)
                zhiMap.put("value", summingTimes.get(et + "治疗费").get("value"));
            map.put("zhiliaoqiuhe_6", zhiMap);
            if (summingTimes.get(et + "治疗费") != null)
                zhicMap.put("value", summingTimes.get(et + "治疗费").get("count"));
            map.put("zhiliaojicishu_2", zhicMap);

            Map<String, Object> zhuMap = new HashMap<>();
            zhuMap.put("unit", "元");
            zhuMap.put("value", 0);
            Map<String, Object> zhucMap = new HashMap<>();
            zhucMap.put("unit", "次");
            zhucMap.put("value", 0);

            if (summingTimes.get(et + "注射费") != null)
                zhuMap.put("value", summingTimes.get(et + "注射费").get("value"));
            map.put("zhusheqiuhe_6", zhuMap);
            if (summingTimes.get(et + "注射费") != null)
                zhucMap.put("value", summingTimes.get(et + "注射费").get("count"));
            map.put("zhushejicishu_2", zhucMap);
            insert.add(map);
            iii++;
            if (insert.size() >= 500) {
                mongoTemplate.insert(insert, huanzhedayku);
                insert = new ArrayList<>();
                System.out.println(iii);
            }
        }

        if (insert.size() > 0) {
            mongoTemplate.insert(insert, huanzhedayku);
            insert = new ArrayList<>();
        }
        System.out.println("ok");
    }


    @Test
    void Diagnosisday() {
        String zhenliaoku = "com.ns.entity.object.form.instance.nsjgpv79iu55j528ig78.zhenliaobiaoanri_1";
        String jiesuanku = "com.ns.entity.object.form.instance.nsjgpv79iu55j528ig78.feiyongjiesuanxinxi_1";
        String mingxiku = "com.ns.entity.object.form.instance.nsjgpv79iu55j528ig78.feiyongmingxixinxi_1";

        Query q1 = new Query();
        q1.fields().include("xingming_103").include("chushengriqi_28")
                .include("nianling_23").include("xingbie_22")
                .include("xianzhongleixing_2").include("danweimingcheng_25")
                .include("ruyuankeshimingcheng_2").include("zhuzhiyishengdaima_1")
                .include("zhuzhiyisheng_1").include("ruyuanjibingmingcheng_3")
                .include("chuyuanjibingmingcheng_3").include("ruyuanriqi_2")
                .include("chuyuanriqi_2").include("zhuyuantianshu_3")
                .include("danjuhao_12").include("chuyuankeshimingcheng_2")
                .include("feiyongjiesuanshijian_5").include("yiliaoleibie_8")
                .exclude("_id");

        List<Map> jiehuanzhe = mongoTemplate.find(q1, Map.class, jiesuanku);

        Map<String, List<Map>> fenliejiesuan = new HashMap<>();
        for (Map k : jiehuanzhe) {
            String fieldValue = k.get("danjuhao_12").toString()
                    + k.get("feiyongjiesuanshijian_5").toString(); // 假设用于分组的字段名为 "fieldName"
            if (fenliejiesuan.get(fieldValue) == null) {
                fenliejiesuan.put(fieldValue, new ArrayList<>());
            }
            fenliejiesuan.get(fieldValue).add(k);
        }
        jiehuanzhe.clear();


        TypedAggregation<Map> TypedAggregation = Aggregation.newAggregation(
                Map.class,
                Aggregation.group("danjuhao_12", "feiyongjiesuanshijian_5")
                        .sum("yiliaofeizonge_6.value").as("money")
                        .sum("tongchouzhifujine_5.value").as("ybmoney")
        ).withOptions(Aggregation.newAggregationOptions().allowDiskUse(true).build());

        AggregationResults<Map> aggregationResults = mongoTemplate.aggregate(
                TypedAggregation,
                jiesuanku,
                Map.class);
        List<Map> mappedResults = aggregationResults.getMappedResults();

        Map<String, Map> encodeTime = new HashMap<>();
        for (Map<String, Map> k : mappedResults) {
            encodeTime.put(k.get("_id").get("danjuhao_12").toString()
                    + k.get("_id").get("feiyongjiesuanshijian_5").toString(), k);
        }

        TypedAggregation<Map> TypedAggregation2 = Aggregation.newAggregation(
                Map.class,
                Aggregation.group("danjuhao_13", "shoufeixiangmuleibie_5", "feiyongjiesuanshijian_6")
                        .sum("jine_20.value").as("value")
                        .count().as("count")
        ).withOptions(Aggregation.newAggregationOptions().allowDiskUse(true).build());
        ;

        AggregationResults<Map> aggregationResults2 = mongoTemplate.aggregate(
                TypedAggregation2,
                mingxiku,
                Map.class);
        List<Map> mappedResults2 = aggregationResults2.getMappedResults();

        Map<String, Map> summingTimes = new HashMap<>();
        for (Map<String, Map> k : mappedResults2) {
            summingTimes.put(k.get("_id").get("danjuhao_13").toString()
                    + k.get("_id").get("feiyongjiesuanshijian_6").toString()
                    + k.get("_id").get("shoufeixiangmuleibie_5").toString(), k);
        }
        TypedAggregation<Map> TypedAggregation3 = Aggregation.newAggregation(
                Map.class,
                Aggregation.group("danjuhao_13", "feiyongjiesuanshijian_6").addToSet("yibaomulubianma_5").as("m3")
        ).withOptions(Aggregation.newAggregationOptions().allowDiskUse(true).build());

        AggregationResults<Map> aggregationResults3 = mongoTemplate.aggregate(
                TypedAggregation3,
                mingxiku,
                Map.class);
        List<Map> mappedResults3 = aggregationResults3.getMappedResults();

        Map<String, Integer> m3 = new HashMap<>();
        for (Map<String, Map> k : mappedResults3) {
            List s = (List) k.get("m3");
            m3.put(k.get("_id").get("danjuhao_13").toString()
                    + k.get("_id").get("feiyongjiesuanshijian_6"), s.size());
        }
        int iii = 0;
        List insert = new ArrayList<>();
        for (String et : encodeTime.keySet()) {
            if (fenliejiesuan.get(et) == null) {
                continue;
            }
            Map<String, Object> map = new HashMap<>();
            //系统字段
            String mainId = UUID.randomUUID().toString();
            map.put("_id", mainId);
            map.put("create_time", System.currentTimeMillis());
            map.put("create_account", "admin");
            map.put("category_id", "469d82fe-a5c7-4c7c-8c10-f1e196012c0b");
            map.put("data_status", "已归档");
            map.put("data_type", 1);
            map.put("priority", "");
            map.put("bind_id", mainId);
            map.put("corp_id", "nsrcu88p7uy22m7i9ioz");
            map.put("parent_corp_id_list", new ArrayList<>());
            map.put("bind_category_id", "");

            if (fenliejiesuan.get(et).get(0).get("danjuhao_12") != null)
                map.put("jiuzhenhao_22", fenliejiesuan.get(et).get(0).get("danjuhao_12"));
            map.put("yibaoniandu_11", 2023);
            if (fenliejiesuan.get(et).get(0).get("yiliaoleibie_8") != null)
                map.put("yiliaoleibie_15", fenliejiesuan.get(et).get(0).get("yiliaoleibie_8"));
            if (fenliejiesuan.get(et).get(0).get("xingming_103") != null)
                map.put("xingming_108", fenliejiesuan.get(et).get(0).get("xingming_103"));
            if (fenliejiesuan.get(et).get(0).get("nianling_23") != null)
                map.put("nianling_26", fenliejiesuan.get(et).get(0).get("nianling_23"));
            if (fenliejiesuan.get(et).get(0).get("xingbie_22") != null)
                map.put("xingbie_25", fenliejiesuan.get(et).get(0).get("xingbie_22"));
            if (fenliejiesuan.get(et).get(0).get("danweimingcheng_25") != null)
                map.put("danweimingcheng_28", fenliejiesuan.get(et).get(0).get("danweimingcheng_25"));
            if (fenliejiesuan.get(et).get(0).get("chushengriqi_28") != null)
                map.put("chushengriqi_31", fenliejiesuan.get(et).get(0).get("chushengriqi_28"));
            map.put("xianzhongleixing_7", "医保");
            map.put("yiyuanmingcheng_11", "孝感市第一人民医院");
            map.put("yiyuanleibie_9", "公立医院");
            map.put("yiyuandengji_9", "三级");
            if (fenliejiesuan.get(et).get(0).get("ruyuankeshimingcheng_2") != null) {
                map.put("keshimingcheng_33", fenliejiesuan.get(et).get(0).get("ruyuankeshimingcheng_2"));
                map.put("zhixingkeshimingcheng_13", fenliejiesuan.get(et).get(0).get("ruyuankeshimingcheng_2"));
            }

            if (fenliejiesuan.get(et).get(0).get("chuyuankeshimingcheng_2") != null) {
                map.put("keshimingcheng_33", fenliejiesuan.get(et).get(0).get("ruyuankeshimingcheng_2"));
                map.put("zhixingkeshimingcheng_13", fenliejiesuan.get(et).get(0).get("ruyuankeshimingcheng_2"));
            }
            if (fenliejiesuan.get(et).get(0).get("zhuzhiyishengdaima_1") != null)
                map.put("yishengbianma_8", fenliejiesuan.get(et).get(0).get("zhuzhiyishengdaima_1"));
            if (fenliejiesuan.get(et).get(0).get("zhuzhiyisheng_1") != null)
                map.put("yishengxingming_6", fenliejiesuan.get(et).get(0).get("zhuzhiyisheng_1"));
            if (fenliejiesuan.get(et).get(0).get("ruyuanjibingmingcheng_3") != null)
                map.put("zhuzhenjibingmingcheng_6", fenliejiesuan.get(et).get(0).get("ruyuanjibingmingcheng_3"));
            if (fenliejiesuan.get(et).get(0).get("chuyuanjibingmingcheng_3") != null)
                map.put("zhuzhenjibingmingcheng_6", fenliejiesuan.get(et).get(0).get("chuyuanjibingmingcheng_3"));
            if (fenliejiesuan.get(et).get(0).get("ruyuanriqi_2") != null)
                map.put("ruyuanriqi_5", fenliejiesuan.get(et).get(0).get("ruyuanriqi_2"));
            if (fenliejiesuan.get(et).get(0).get("chuyuanriqi_2") != null)
                map.put("chuyuanriqi_5", fenliejiesuan.get(et).get(0).get("chuyuanriqi_2"));
            if (fenliejiesuan.get(et).get(0).get("zhuyuantianshu_3") != null)
                map.put("zhuyuantianshu_6", fenliejiesuan.get(et).get(0).get("zhuyuantianshu_3"));
            Map zje = new HashMap<>();
            Double jin = 0.0, yjin = 0.0;
            zje.put("unit", "元");
            zje.put("value", 0);
            if (encodeTime.get(et).get("money") != null) {
                jin = Double.parseDouble(encodeTime.get(et).get("money").toString());
                zje.put("value", jin);
            }
            map.put("zongjine_10", zje);
            if (encodeTime.get(et).get("ybmoney") != null) {
                yjin = Double.parseDouble(encodeTime.get(et).get("ybmoney").toString());
                zje.put("value", yjin);
            }
            map.put("yibaofanweifeiyong_10", zje);
            //住院药品种类数量
            HashMap zyz = new HashMap();
            zyz.put("unit", "种");
            zyz.put("value", 0);
            if (m3.get(et) != null)
                zyz.put("value", m3.get(et));
            map.put("zhuyuanyaopinzhongleishuliang_2", zyz);

            //CT求和
            Map<String, Object> CTMap = new HashMap<>();
            CTMap.put("unit", "元");
            CTMap.put("value", 0);
            Map<String, Object> CTcMap = new HashMap<>();
            CTcMap.put("unit", "次");
            CTcMap.put("value", 0);

            if (summingTimes.get(et + "CT费") != null) {
                CTMap.put("value", summingTimes.get(et + "CT费").get("value"));
                CTcMap.put("value", summingTimes.get(et + "CT费").get("count"));
            }
            map.put("CTqiuhe_4", CTMap);
            map.put("CTjici_4", CTcMap);

            Map<String, Object> TCDMap = new HashMap<>();
            TCDMap.put("unit", "元");
            TCDMap.put("value", 0);
            Map<String, Object> TCDcMap = new HashMap<>();
            TCDcMap.put("unit", "次");
            TCDcMap.put("value", 0);

            if (summingTimes.get(et + "TCD") != null)
                TCDMap.put("value", summingTimes.get(et + "TCD").get("value"));
            map.put("TCDqiuhe_4", TCDMap);
            if (summingTimes.get(et + "TCD") != null)
                TCDcMap.put("value", summingTimes.get(et + "TCD").get("count"));
            map.put("TCDjici_4", TCDcMap);

            Map<String, Object> bingliMap = new HashMap<>();
            bingliMap.put("unit", "元");
            bingliMap.put("value", 0);
            Map<String, Object> bcMap = new HashMap<>();
            bcMap.put("unit", "次");
            bcMap.put("value", 0);

            if (summingTimes.get(et + "病理费") != null)
                bingliMap.put("value", summingTimes.get(et + "病理费").get("value"));
            map.put("bingliqiuhe_4", bingliMap);
            if (summingTimes.get(et + "病理费") != null)
                bcMap.put("value", summingTimes.get(et + "病理费").get("count"));
            map.put("binglijici_4", bcMap);

            Map<String, Object> cailiaoMap = new HashMap<>();
            cailiaoMap.put("unit", "元");
            cailiaoMap.put("value", 0);
            Map<String, Object> caicMap = new HashMap<>();
            caicMap.put("unit", "次");
            caicMap.put("value", 0);

            if (summingTimes.get(et + "材料费") != null)
                cailiaoMap.put("value", summingTimes.get(et + "材料费").get("value"));
            map.put("cailiaoqiuhe_4", cailiaoMap);
            if (summingTimes.get(et + "材料费") != null)
                caicMap.put("value", summingTimes.get(et + "材料费").get("count"));
            map.put("cailiaojici_4", caicMap);

            Map<String, Object> caichaoMap = new HashMap<>();
            caichaoMap.put("unit", "元");
            caichaoMap.put("value", 0);
            Map<String, Object> cailcMap = new HashMap<>();
            cailcMap.put("unit", "次");
            cailcMap.put("value", 0);

            if (summingTimes.get(et + "彩超费") != null)
                caichaoMap.put("value", summingTimes.get(et + "彩超费").get("value"));
            map.put("caichaoqiuhe_4", caichaoMap);
            if (summingTimes.get(et + "彩超费") != null)
                cailcMap.put("value", summingTimes.get(et + "彩超费").get("count"));
            map.put("caichaojici_4", cailcMap);


            Map<String, Object> caoyaoMap = new HashMap<>();
            caoyaoMap.put("unit", "元");
            caoyaoMap.put("value", 0);
            Map<String, Object> caocMap = new HashMap<>();
            caocMap.put("unit", "次");
            caocMap.put("value", 0);

            if (summingTimes.get(et + "草药费") != null)
                caoyaoMap.put("value", summingTimes.get(et + "草药费").get("value"));
            map.put("caoyaoqiuhe_4", caoyaoMap);
            if (summingTimes.get(et + "草药费") != null)
                caocMap.put("value", summingTimes.get(et + "草药费").get("count"));
            map.put("caoyaojici_4", caocMap);


            Map<String, Object> zhongchengMap = new HashMap<>();
            zhongchengMap.put("unit", "元");
            zhongchengMap.put("value", 0);
            Map<String, Object> zcMap = new HashMap<>();
            zcMap.put("unit", "次");
            zcMap.put("value", 0);


            if (summingTimes.get(et + "中成药费") != null)
                zhongchengMap.put("value", summingTimes.get(et + "中成药费").get("value"));
            map.put("zhongchengyaoqiuhe_4", zhongchengMap);
            if (summingTimes.get(et + "中成药费") != null)
                zcMap.put("value", summingTimes.get(et + "中成药费").get("count"));
            map.put("zhongchengyaojici_4", zcMap);


            Map<String, Object> ciMap = new HashMap<>();
            ciMap.put("unit", "元");
            ciMap.put("value", 0);
            Map<String, Object> cicMap = new HashMap<>();
            cicMap.put("unit", "次");
            cicMap.put("value", 0);

            if (summingTimes.get(et + "磁共振") != null)
                ciMap.put("value", summingTimes.get(et + "磁共振").get("value"));
            map.put("cigongzhenqiuhe_4", ciMap);
            if (summingTimes.get(et + "磁共振") != null)
                cicMap.put("value", summingTimes.get(et + "磁共振").get("count"));
            map.put("cigongzhenjici_4", cicMap);


            Map<String, Object> huMap = new HashMap<>();
            huMap.put("unit", "元");
            huMap.put("value", 0);
            Map<String, Object> hucMap = new HashMap<>();
            hucMap.put("unit", "次");
            hucMap.put("value", 0);

            if (summingTimes.get(et + "护理费") != null)
                huMap.put("value", summingTimes.get(et + "护理费").get("value"));
            map.put("huliqiuhe_4", huMap);
            if (summingTimes.get(et + "护理费") != null)
                hucMap.put("value", summingTimes.get(et + "护理费").get("count"));
            map.put("hulijici_4", hucMap);

            Map<String, Object> huaMap = new HashMap<>();
            huaMap.put("unit", "元");
            huaMap.put("value", 0);
            Map<String, Object> huacMap = new HashMap<>();
            huacMap.put("unit", "次");
            huacMap.put("value", 0);


            if (summingTimes.get(et + "化验费") != null)
                huaMap.put("value", summingTimes.get(et + "化验费").get("value"));
            map.put("huayanqiuhe_4", huaMap);
            if (summingTimes.get(et + "化验费") != null)
                huacMap.put("value", summingTimes.get(et + "化验费").get("count"));
            map.put("huayanjici_4", huacMap);

            Map<String, Object> huanMap = new HashMap<>();
            huanMap.put("unit", "元");
            huanMap.put("value", 0);
            Map<String, Object> huancMap = new HashMap<>();
            huancMap.put("unit", "次");
            huancMap.put("value", 0);

            if (summingTimes.get(et + "换药费") != null)
                huanMap.put("value", summingTimes.get(et + "换药费").get("value"));
            map.put("huanyaoqiuhe_4", huanMap);
            if (summingTimes.get(et + "换药费") != null)
                huancMap.put("value", summingTimes.get(et + "换药费").get("count"));
            map.put("huanyaojici_4", huancMap);

            Map<String, Object> jianMap = new HashMap<>();
            jianMap.put("unit", "元");
            jianMap.put("value", 0);
            Map<String, Object> jiancMap = new HashMap<>();
            jiancMap.put("unit", "次");
            jiancMap.put("value", 0);

            if (summingTimes.get(et + "检查费") != null)
                jianMap.put("value", summingTimes.get(et + "检查费").get("value"));
            map.put("jianchaqiuhe_4", jianMap);
            if (summingTimes.get(et + "检查费") != null)
                jiancMap.put("value", summingTimes.get(et + "检查费").get("count"));
            map.put("jianchajici_4", jiancMap);

            Map<String, Object> jiuMap = new HashMap<>();
            jiuMap.put("unit", "元");
            jiuMap.put("value", 0);
            Map<String, Object> jiucMap = new HashMap<>();
            jiucMap.put("unit", "次");
            jiucMap.put("value", 0);

            if (summingTimes.get(et + "救护车") != null)
                jiuMap.put("value", summingTimes.get(et + "救护车").get("value"));
            map.put("jiuhucheqiuhe_4", jiuMap);
            if (summingTimes.get(et + "救护车") != null)
                jiucMap.put("value", summingTimes.get(et + "救护车").get("count"));
            map.put("jiuhuchejici_4", jiucMap);

            Map<String, Object> liliaoMap = new HashMap<>();
            liliaoMap.put("unit", "元");
            liliaoMap.put("value", 0);
            Map<String, Object> liliaocMap = new HashMap<>();
            liliaocMap.put("unit", "次");
            liliaocMap.put("value", 0);

            if (summingTimes.get(et + "理疗车") != null)
                liliaoMap.put("value", summingTimes.get(et + "理疗费").get("value"));
            map.put("liliaoqiuhe_4", liliaoMap);
            if (summingTimes.get(et + "理疗车") != null)
                liliaocMap.put("value", summingTimes.get(et + "理疗费").get("count"));
            map.put("liliaojici_4", liliaocMap);

            Map<String, Object> maMap = new HashMap<>();
            maMap.put("unit", "元");
            maMap.put("value", 0);
            Map<String, Object> macMap = new HashMap<>();
            macMap.put("unit", "次");
            macMap.put("value", 0);

            if (summingTimes.get(et + "麻醉费") != null)
                maMap.put("value", summingTimes.get(et + "麻醉费").get("value"));
            map.put("mazuiqiuhe_4", maMap);
            if (summingTimes.get(et + "麻醉费") != null)
                macMap.put("value", summingTimes.get(et + "麻醉费").get("count"));
            map.put("mazuijici_4", macMap);

            Map<String, Object> naoMap = new HashMap<>();
            naoMap.put("unit", "元");
            naoMap.put("value", 0);
            Map<String, Object> naocMap = new HashMap<>();
            naocMap.put("unit", "次");
            naocMap.put("value", 0);

            if (summingTimes.get(et + "脑电费") != null)
                naoMap.put("value", summingTimes.get(et + "脑电费").get("value"));
            map.put("naodiantuqiuhe_4", naoMap);
            if (summingTimes.get(et + "脑电费") != null)
                naocMap.put("value", summingTimes.get(et + "脑电费").get("count"));
            map.put("naodiantujici_4", naocMap);

            Map<String, Object> paiMap = new HashMap<>();
            paiMap.put("unit", "元");
            paiMap.put("value", 0);
            Map<String, Object> paicMap = new HashMap<>();
            paicMap.put("unit", "次");
            paicMap.put("value", 0);

            if (summingTimes.get(et + "拍片费") != null)
                paiMap.put("value", summingTimes.get(et + "拍片费").get("value"));
            map.put("paipianqiuhe_4", paiMap);
            if (summingTimes.get(et + "拍片费") != null)
                paicMap.put("value", summingTimes.get(et + "拍片费").get("count"));
            map.put("paipianjici_4", paicMap);

            Map<String, Object> ssMap = new HashMap<>();
            ssMap.put("unit", "元");
            ssMap.put("value", 0);
            Map<String, Object> sscMap = new HashMap<>();
            sscMap.put("unit", "次");
            sscMap.put("value", 0);

            if (summingTimes.get(et + "手术材料费") != null)
                ssMap.put("value", summingTimes.get(et + "手术材料费").get("value"));
            map.put("shoushucailiaoqiuhe_4", ssMap);
            if (summingTimes.get(et + "手术材料费") != null)
                sscMap.put("value", summingTimes.get(et + "手术材料费").get("count"));
            map.put("shoushucailiaojici_4", sscMap);

            Map<String, Object> shouMap = new HashMap<>();
            shouMap.put("unit", "元");
            shouMap.put("value", 0);
            Map<String, Object> shoucMap = new HashMap<>();
            shoucMap.put("unit", "次");
            shoucMap.put("value", 0);


            if (summingTimes.get(et + "手术费") != null)
                shouMap.put("value", summingTimes.get(et + "手术费").get("value"));
            map.put("shoushuqiuhe_4", shouMap);
            if (summingTimes.get(et + "手术费") != null)
                shoucMap.put("value", summingTimes.get(et + "手术费").get("count"));
            map.put("shoushujici_4", shoucMap);


            Map<String, Object> tiMap = new HashMap<>();
            tiMap.put("unit", "元");
            tiMap.put("value", 0);
            Map<String, Object> ticMap = new HashMap<>();
            ticMap.put("unit", "次");
            ticMap.put("value", 0);

            if (summingTimes.get(et + "体检费") != null)
                tiMap.put("value", summingTimes.get(et + "体检费").get("value"));
            map.put("tijianqiuhe_4", tiMap);
            if (summingTimes.get(et + "体检费") != null)
                ticMap.put("value", summingTimes.get(et + "体检费").get("count"));
            map.put("tijianjici_4", ticMap);


            Map<String, Object> weiMap = new HashMap<>();
            weiMap.put("unit", "元");
            weiMap.put("value", 0);
            Map<String, Object> weicMap = new HashMap<>();
            weicMap.put("unit", "次");
            weicMap.put("value", 0);

            if (summingTimes.get(et + "胃镜费") != null)
                weiMap.put("value", summingTimes.get(et + "胃镜费").get("value"));
            map.put("weijingqiuhe_4", weiMap);
            if (summingTimes.get(et + "胃镜费") != null)
                weicMap.put("value", summingTimes.get(et + "胃镜费").get("count"));
            map.put("weijingjici_4", weicMap);

            Map<String, Object> xiMap = new HashMap<>();
            xiMap.put("unit", "元");
            xiMap.put("value", 0);
            Map<String, Object> xicMap = new HashMap<>();
            xicMap.put("unit", "次");
            xicMap.put("value", 0);

            if (summingTimes.get(et + "西药费") != null)
                xiMap.put("value", summingTimes.get(et + "西药费").get("value"));
            map.put("xiyaoqiuhe_4", xiMap);
            if (summingTimes.get(et + "西药费") != null)
                xicMap.put("value", summingTimes.get(et + "西药费").get("count"));
            map.put("xiyaojici_4", xicMap);

            Map<String, Object> xinMap = new HashMap<>();
            xinMap.put("unit", "元");
            xinMap.put("value", 0);
            Map<String, Object> xincMap = new HashMap<>();
            xincMap.put("unit", "次");
            xincMap.put("value", 0);

            if (summingTimes.get(et + "心超费") != null)
                xinMap.put("value", summingTimes.get(et + "心超费").get("value"));
            map.put("xinchaoqiuhe_4", xinMap);
            if (summingTimes.get(et + "心超费") != null)
                xincMap.put("value", summingTimes.get(et + "心超费").get("count"));
            map.put("xinchaojici_4", xincMap);

            Map<String, Object> zhenMap = new HashMap<>();
            zhenMap.put("unit", "元");
            zhenMap.put("value", 0);
            Map<String, Object> zhencMap = new HashMap<>();
            zhencMap.put("unit", "次");
            zhencMap.put("value", 0);

            if (summingTimes.get(et + "诊疗费") != null)
                zhenMap.put("value", summingTimes.get(et + "诊疗费").get("value"));
            map.put("zhenliaoqiuhe_4", zhenMap);
            if (summingTimes.get(et + "诊疗费") != null)
                zhencMap.put("value", summingTimes.get(et + "诊疗费").get("count"));
            map.put("zhenliaojici_4", zhencMap);

            Map<String, Object> zhiMap = new HashMap<>();
            zhiMap.put("unit", "元");
            zhiMap.put("value", 0);
            Map<String, Object> zhicMap = new HashMap<>();
            zhicMap.put("unit", "次");
            zhicMap.put("value", 0);

            if (summingTimes.get(et + "治疗费") != null)
                zhiMap.put("value", summingTimes.get(et + "治疗费").get("value"));
            map.put("zhiliaoqiuhe_4", zhiMap);
            if (summingTimes.get(et + "治疗费") != null)
                zhicMap.put("value", summingTimes.get(et + "治疗费").get("count"));
            map.put("zhiliaojici_4", zhicMap);

            Map<String, Object> zhuMap = new HashMap<>();
            zhuMap.put("unit", "元");
            zhuMap.put("value", 0);
            Map<String, Object> zhucMap = new HashMap<>();
            zhucMap.put("unit", "次");
            zhucMap.put("value", 0);

            if (summingTimes.get(et + "注射费") != null)
                zhuMap.put("value", summingTimes.get(et + "注射费").get("value"));
            map.put("zhusheqiuhe_4", zhuMap);
            if (summingTimes.get(et + "注射费") != null)
                zhucMap.put("value", summingTimes.get(et + "注射费").get("count"));
            map.put("zhushejici_4", zhucMap);

            //诊疗项目金额
            if (map.get("zhenliaoqiuhe_4") != null)
                map.put("zhenliaoxiangmujine_2", map.get("zhenliaoqiuhe_4"));
            if (fenliejiesuan.get(et).get(0).get("feiyongjiesuanshijian_5") != null)
                map.put("fashengriqi_8", fenliejiesuan.get(et).get(0).get("feiyongjiesuanshijian_5"));

            iii++;
            insert.add(map);
            if (insert.size() >= 1000) {
                mongoTemplate.insert(insert, zhenliaoku);
                insert = new ArrayList<>();
                System.out.println(iii);
            }
        }
        if (insert.size() > 0) {
            mongoTemplate.insert(insert, zhenliaoku);
            insert = new ArrayList<>();
        }
    }

    @Test
    void sss() {
        TypedAggregation<Map> TypedAggregation2 = Aggregation.newAggregation(
                Map.class,
                Aggregation.group("diag_dr_name").addToSet("diag_dr_code").as("codes")
                        .addToSet("diag_name").as("diag_name"),
                Aggregation.unwind("codes"),
                Aggregation.sort(Sort.Direction.ASC, "codes"),
                Aggregation.project("_id", "codes"),
                Aggregation.group("_id").addToSet("codes").as("codes")
        );
        AggregationResults<Map> aggregationResults2 = mongoTemplate.aggregate(
                TypedAggregation2,
                "测试",
                Map.class);

        List<Map> mappedResults2 = aggregationResults2.getMappedResults();
    }

    @Test
    void openCsv() throws Exception {
        int size = 12;
        {
            try {
                //初始化线程安全集合存储数据 一条行数据对应一个List<String>
                List<List<String>> list = new ArrayList<>();
                // 拼接换行数据需要
                Collection<String> joint = Collections.synchronizedCollection(new ArrayList<>());
                // 获取文件
                File touch = FileUtil.touch("D:\\桌面\\换行.csv");
                // 创建一个文件输入流
                FileInputStream fileInputStream = IoUtil.toStream(touch);
                // 创建一个可以读取UTF-8编码的BufferedReader
                BufferedReader utf8Reader = IoUtil.getUtf8Reader(fileInputStream);
                // 读取所有行并转换为Stream(延迟求值) 需要处理才会读取
                Stream<String> lines = utf8Reader.lines();
                //System.out.println(lines.count());
                // 对每一行进行并行处理
                lines.parallel().forEach(s -> {
                    synchronized (this){
                        List<String> next = new ArrayList<>();
                        try (CSVReader reader = new CSVReader(new StringReader(s))) {
                            // 读取下一行并添加到列表中
                            next = Arrays.asList(reader.readNext());
                            if(next.size()<12)
                                System.out.println("xx");
                            list.add(next);
                        } catch (IOException | CsvValidationException e) {
                            //利用csv引号异常判断出换行
                            joint.addAll(next);
                            if (joint.size() == size) {
                                List<String> jointCopy = new ArrayList<>(joint); // 创建joint的副本
                                System.out.println("换行"+jointCopy);
                                joint.clear();
                                list.add(jointCopy);
                            }
                        }
                    }
                });
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


    @Autowired
    private Environment environment;
    @Test
    void mm(){
        System.out.println(environment.getProperty("zhenduan.eposide_id"));
    }
}
