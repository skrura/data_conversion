package com.example.data_conversion;


import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import sun.management.resources.agent;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
//@ContextConfiguration(classes =  DataConversionApplicationTests.class)
class DataConversionApplicationTests {

    @Autowired(required = false)
    private MongoTemplate mongoTemplate;

    @Test
    public void one() {
        System.out.println("1");
    }

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

            zhuyuanzhubiao =  mongoTemplate.findAll(Map.class,zhuyuanzhubiaoku);

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
        map.put("churuyuanzhenduanleibie_2","2");
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
        map0.put("churuyuanzhenduanleibie_2","3");
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
    void zhneduanxinx_menzhen()
    {
        List<Map> menzhenzhubiao = new ArrayList<>();

        String menzhenzhubiaoku = "com.ns.entity.object.form.instance.nsjgpv79iu55j528ig78.xiaoganshidiyirenminyiyuanmenzhenzhudan_1";
        String zhenduanxinxiku = "com.ns.entity.object.form.instance.nsjgpv79iu55j528ig78.zhenduanxinxi_2";
        String empty = "";
        List<Map<String, Object>> insertList = new ArrayList<>();
        menzhenzhubiao = mongoTemplate.findAll(Map.class,menzhenzhubiaoku);
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
    void feiyongjiesuanclear(){
        String feiyongjiesuanku = "com.ns.entity.object.form.instance.nsjgpv79iu55j528ig78.feiyongjiesuanxinxi_1";
        List<Map> all = mongoTemplate.findAll(Map.class,feiyongjiesuanku);
        if(all.size()>0){
            mongoTemplate.remove(new Query(),feiyongjiesuanku);
           // System.out.println(all);
        }
    }
    @Test
    void feiyongjiesuancount(){
        String zhuyuanzhubiaoku = "com.ns.entity.object.form.instance.nsjgpv79iu55j528ig78.xiaoganshidiyirenminyiyuanzhuyuanzhudan_2";
        String feiyongjiesuanku = "com.ns.entity.object.form.instance.nsjgpv79iu55j528ig78.feiyongjiesuanxinxi_1";
        String menzhenzhubiaoku = "com.ns.entity.object.form.instance.nsjgpv79iu55j528ig78.xiaoganshidiyirenminyiyuanmenzhenzhudan_1";


        System.out.println("门诊主单："+mongoTemplate.count(new Query(), menzhenzhubiaoku));
        System.out.println("住院主单："+mongoTemplate.count(new Query(), zhuyuanzhubiaoku));
        System.out.println("费用结算："+mongoTemplate.count(new Query(), feiyongjiesuanku));
    }

    @Test
    void feiyongjiesuan_zhuyuan(){
        //数据列表
        List<Map> zhuyuanzhubiao = new ArrayList<>();

        String zhuyuanzhubiaoku = "com.ns.entity.object.form.instance.nsjgpv79iu55j528ig78.xiaoganshidiyirenminyiyuanzhuyuanzhudan_2";
        String feiyongjiesuanku = "com.ns.entity.object.form.instance.nsjgpv79iu55j528ig78.feiyongjiesuanxinxi_1";

        zhuyuanzhubiao =  mongoTemplate.findAll(Map.class,zhuyuanzhubiaoku);

        //Map shu = mongoTemplate.findOne(new Query(Criteria.where("jiesuandanjuhao_3").is("5150667")), Map.class, zhuyuanzhubiaoku);
        String empty = "";

        //入库
        List<Map<String, Object>> insertList = new ArrayList<>();

        for (Map shu: zhuyuanzhubiao){
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
            map.put("tongchouqumingcheng_5",empty);
            map.put("canbaorentongchouqumingcheng_5",empty);
            map.put("yibaoniandu_5",empty);
            map.put("dingdianjigoubianma_5",shu.get("yiliaojigoubianma_8"));
            map.put("dingdianjigoumingcheng_5",shu.get("yiliaojigoumingcheng_8"));
            map.put("shehuibaozhangkahao_5",empty);
            map.put("zhengjianhaoma_22",empty);
            map.put("renyuanbianhao_2",shu.get("gerenbianma_4"));
            map.put("xingming_103",shu.get("huanzhexingming_6"));
            map.put("chushengriqi_28",shu.get("huanzhechushengriqi_3"));
            map.put("nianling_23",shu.get("huanzhenianling_3"));
            map.put("xingbie_22",shu.get("huanzhexingbie_4"));
            map.put("danweimingcheng_25",shu.get("canbaorendanweidizhi_2"));
            map.put("jiatingzhuzhi_6",empty);
            map.put("lianxidianhua_26",empty);
            map.put("renyuanleibie_8",empty);
            map.put("xianzhongleixing_2",shu.get("xianzhongleixing_6"));
            map.put("yiliaoleibie_8","住院");
            map.put("jiuzhenhao_15",empty);
            map.put("danjuhao_12",shu.get("jiesuandanjuhao_3"));
            map.put("binganshouyehao_3",empty);
            map.put("feiyongjiesuanshijian_5",shu.get("jiesuanriqi_7"));
            map.put("yiliaofeizonge_6",shu.get("zongfeiyong_3"));

            Map<String, Object> zhifuMoney = new HashMap<>();
            zhifuMoney.put("unit", "元");
            zhifuMoney.put("value", 0.0);

            map.put("gerenzhanghuzhifu_8",zhifuMoney);

            Map<String, Object> xianjinMoney = new HashMap<>();
            xianjinMoney.put("unit", "元");
             xianjinMoney.put("value",0.0);

            map.put("gerenxianjinzhifu_7",xianjinMoney);

            map.put("tongchouzhifujine_5",shu.get("jibentongchouzhifu_2"));

            Map<String, Object> dabingMoney = new HashMap<>();
            dabingMoney.put("unit", "元");
            dabingMoney.put("value", 0.0);

            map.put("dabingzhifu_3",dabingMoney);

            Map<String, Object> gongwuMoney = new HashMap<>();
            gongwuMoney.put("unit", "元");
            gongwuMoney.put("value", 0.0);

            map.put("gongwuyuanbuzhu_3",gongwuMoney);

            Map<String, Object>minzhengMoney = new HashMap<>();
            minzhengMoney.put("unit", "元");
            minzhengMoney.put("value",0.0);

            map.put("minzhengjijin_3",minzhengMoney);

            Map<String, Object> canlianMoney = new HashMap<>();
            canlianMoney.put("unit", "元");
            canlianMoney.put("value", 0.0);

            map.put("canlianjijin_3",canlianMoney);

            Map<String, Object> qitaMoney = new HashMap<>();
            qitaMoney.put("unit", "元");
            qitaMoney.put("value",0.0);

            map.put("qitabuzhu_3",qitaMoney);

            Map<String, Object> jiatingMoney = new HashMap<>();
            jiatingMoney.put("unit", "元");
            jiatingMoney.put("value",0.0);

            map.put("jiatinggongjizhanghuzhifu_3",jiatingMoney);

            Map<String, Object> dangnianMoney = new HashMap<>();
            dangnianMoney.put("unit", "元");
            dangnianMoney.put("value",0.0);

            map.put("dangnianzhanghuzhifue_3",dangnianMoney);

            Map<String, Object> linianMoney = new HashMap<>();
            linianMoney.put("unit", "元");
            linianMoney.put("value",0.0);

            map.put("linianzhanghuzhifue_3",linianMoney);

            Map<String, Object> zifeiMoney = new HashMap<>();
            zifeiMoney.put("unit", "元");
            zifeiMoney.put("value", 0.0);

            map.put("gerenzifei_3",zifeiMoney);


            Map<String, Object> ziliMoney = new HashMap<>();
            ziliMoney.put("unit", "元");
            ziliMoney.put("value",0.0);

            map.put("gerenzili_1",ziliMoney);

            Map<String, Object> zifuMoney = new HashMap<>();
            zifuMoney.put("unit", "元");
            zifuMoney.put("value",0.0);

            map.put("gerenzifu_5",zifuMoney);

            Map<String, Object> jiesuanqiandangMoney = new HashMap<>();
            jiesuanqiandangMoney.put("unit", "元");
            jiesuanqiandangMoney.put("value",0.0);

            map.put("jiesuanqiandangnianzhanghuyue_3",jiesuanqiandangMoney);

            Map<String, Object> jiesuanqianliMoney = new HashMap<>();
            jiesuanqianliMoney.put("unit", "元");
            jiesuanqianliMoney.put("value",0.0);

            map.put("jiesuanqianlinianzhanghuyue_3",jiesuanqianliMoney);


            map.put("teshubingzhongbiaoshi_5",empty);
            map.put("jiesuanzhuangtai_7",empty);
            map.put("jiesuanfangshi_21",empty);
            map.put("yibaofufeifangshi_3",empty);
            map.put("yiliaofeiyongzhifufangshi_3",empty);
            map.put("fapiaohaoma_12",empty);
            map.put("chuangweihao_5",shu.get("zhuyuanchuangweihao_2"));
            map.put("ruyuanriqi_2",shu.get("ruyuanriqi_8"));
            map.put("chuyuanriqi_2",shu.get("chuyuanriqi_8"));
            map.put("zhuyuantianshu_3",shu.get("zhuyuantianshu_7"));
            map.put("ruyuanzhenduanjibingbianma_3",shu.get("ruyuanzhenduanbianma_3"));
            map.put("ruyuanjibingmingcheng_3",shu.get("ruyuanzhenduanmingcheng_2"));
            map.put("chuyuanjibingzhenduanbianma_3",shu.get("chuyuanzhenduanbianma_2"));
            map.put("chuyuanjibingmingcheng_3",shu.get("chuyuanzhenduanmingcheng_2"));
            map.put("ruyuankeshimingcheng_2",shu.get("ruyuankeshimingcheng_4"));
            map.put("chuyuankeshimingcheng_2",shu.get("chuyuankeshimingcheng_3"));
            map.put("zhuzhiyishengdaima_1",shu.get("yishengbianhao_2"));
            map.put("zhuzhiyisheng_1",shu.get("yishengxingming_6"));
            map.put("liyuanfangshi_7",empty);
            map.put("chongxiaodanjuhao_3",empty);
            map.put("yidijiuzhenbiaozhi_1",empty);

            insertList.add(map);

            if(insertList.size() >= 1500){
                mongoTemplate.insert(insertList,feiyongjiesuanku);
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
    void feiyongjiesuan_menzhen(){
        List<Map> menzhenzhubiao = new ArrayList<>();

        String menzhenzhubiaoku = "com.ns.entity.object.form.instance.nsjgpv79iu55j528ig78.xiaoganshidiyirenminyiyuanmenzhenzhudan_1";
        String feiyongjiesuanku = "com.ns.entity.object.form.instance.nsjgpv79iu55j528ig78.feiyongjiesuanxinxi_1";

        //Map shu = mongoTemplate.findOne(new Query(Criteria.where("yibaojiesuandanjuhao_1").is("400z001")), Map.class, menzhenzhubiaoku);
        menzhenzhubiao = mongoTemplate.findAll(Map.class,menzhenzhubiaoku);
        String empty = "";

        //入库
        List<Map<String, Object>> insertList = new ArrayList<>();
        for (Map shu: menzhenzhubiao){
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
            map.put("tongchouqumingcheng_5",empty);
            map.put("canbaorentongchouqumingcheng_5",empty);
            map.put("yibaoniandu_5",empty);
            map.put("dingdianjigoubianma_5",shu.get("yiliaojigoubianma_9"));
            map.put("dingdianjigoumingcheng_5",shu.get("yiliaojigoumingcheng_9"));
            map.put("shehuibaozhangkahao_5",empty);
            map.put("zhengjianhaoma_22",empty);
            map.put("renyuanbianhao_2",shu.get("gerenbianma_5"));
            map.put("xingming_103",shu.get("huanzhexingming_7"));
            map.put("chushengriqi_28",shu.get("huanzhechushengriqi_4"));
            map.put("nianling_23",shu.get("huanzhenianling_4"));
            map.put("xingbie_22",shu.get("huanzhexingbie_5"));
            map.put("danweimingcheng_25",empty);
            map.put("jiatingzhuzhi_6",empty);
            map.put("lianxidianhua_26",empty);
            map.put("renyuanleibie_8",empty);
            map.put("xianzhongleixing_2",shu.get("jiesuanleibie_2"));
            map.put("yiliaoleibie_8","普通门诊");
            map.put("jiuzhenhao_15",empty);
            map.put("danjuhao_12",shu.get("yibaojiesuandanjuhao_1"));
            map.put("binganshouyehao_3",empty);
            map.put("feiyongjiesuanshijian_5",shu.get("jiesuanriqi_8"));
            map.put("yiliaofeizonge_6",shu.get("yiliaozongfashengfeiyong_2"));
            map.put("gerenzhanghuzhifu_8",shu.get("gerenzhanghuzhifu_6"));

            Map<String, Object> xianjinMoney = new HashMap<>();
            xianjinMoney.put("unit", "元");
            xianjinMoney.put("value", 0.0);

            map.put("gerenxianjinzhifu_7",xianjinMoney);


            map.put("tongchouzhifujine_5",shu.get("yibaotongchoujijinzhifufeiyong_2"));

            Map<String, Object> dabingMoney = new HashMap<>();
            dabingMoney.put("unit", "元");
            dabingMoney.put("value", 0.0);

            map.put("dabingzhifu_3",dabingMoney);

            Map<String, Object> gongwuMoney = new HashMap<>();
            gongwuMoney.put("unit", "元");
            gongwuMoney.put("value", 0.0);

            map.put("gongwuyuanbuzhu_3",gongwuMoney);

            Map<String, Object>minzhengMoney = new HashMap<>();
            minzhengMoney.put("unit", "元");
            minzhengMoney.put("value", 0.0);

            map.put("minzhengjijin_3",minzhengMoney);

            Map<String, Object> canlianMoney = new HashMap<>();
            canlianMoney.put("unit", "元");
            canlianMoney.put("value", 0.0);

            map.put("canlianjijin_3",canlianMoney);

            Map<String, Object> qitaMoney = new HashMap<>();
            qitaMoney.put("unit", "元");
            qitaMoney.put("value", 0.0);

            map.put("qitabuzhu_3",qitaMoney);

            Map<String, Object> jiatingMoney = new HashMap<>();
            jiatingMoney.put("unit", "元");
            jiatingMoney.put("value", 0.0);

            map.put("jiatinggongjizhanghuzhifu_3",jiatingMoney);

            Map<String, Object> dangnianMoney = new HashMap<>();
            dangnianMoney.put("unit", "元");
            dangnianMoney.put("value", 0.0);

            map.put("dangnianzhanghuzhifue_3",dangnianMoney);

            Map<String, Object> linianMoney = new HashMap<>();
            linianMoney.put("unit", "元");
            linianMoney.put("value",0.0);

            map.put("linianzhanghuzhifue_3",linianMoney);

            Map<String, Object> zifeiMoney = new HashMap<>();
            zifeiMoney.put("unit", "元");
            zifeiMoney.put("value", 0.0);

            map.put("gerenzifei_3",zifeiMoney);

            Map<String, Object> ziliMoney = new HashMap<>();
            ziliMoney.put("unit", "元");
            ziliMoney.put("value", 0.0);

            map.put("gerenzili_1",ziliMoney);

            Map<String, Object> zifuMoney = new HashMap<>();
            zifuMoney.put("unit", "元");
            zifuMoney.put("value", 0.0);

            map.put("gerenzifu_5",zifuMoney);

            Map<String, Object> jiesuanqiandangMoney = new HashMap<>();
            jiesuanqiandangMoney.put("unit", "元");
            jiesuanqiandangMoney.put("value", 0.0);

            map.put("jiesuanqiandangnianzhanghuyue_3",jiesuanqiandangMoney);

            Map<String, Object> jiesuanqianliMoney = new HashMap<>();
            jiesuanqianliMoney.put("unit", "元");
            jiesuanqianliMoney.put("value", 0.0);

            map.put("jiesuanqianlinianzhanghuyue_3",jiesuanqianliMoney);

            map.put("teshubingzhongbiaoshi_5",empty);
            map.put("jiesuanzhuangtai_7",empty);
            map.put("jiesuanfangshi_21",empty);
            map.put("yibaofufeifangshi_3",empty);
            map.put("yiliaofeiyongzhifufangshi_3",empty);
            map.put("fapiaohaoma_12",empty);
            map.put("chuangweihao_5",empty);
            map.put("ruyuanriqi_2",empty);
            map.put("chuyuanriqi_2",empty);

            Map<String, Object> zhuyuantianshu = new HashMap<>();
            zhuyuantianshu.put("unit", "天");
            zhuyuantianshu.put("value", 0);

            map.put("zhuyuantianshu_3",zhuyuantianshu);

            map.put("ruyuanzhenduanjibingbianma_3",shu.get("zhenduanbianma_4"));
            map.put("ruyuanjibingmingcheng_3",shu.get("zhenduanmingcheng_4"));
            map.put("chuyuanjibingzhenduanbianma_3",empty);
            map.put("chuyuanjibingmingcheng_3",empty);
            map.put("ruyuankeshimingcheng_2",shu.get("keshimingcheng_24"));
            map.put("chuyuankeshimingcheng_2",empty);
            map.put("zhuzhiyishengdaima_1",shu.get("zhuzhenyishibianma_2"));
            map.put("zhuzhiyisheng_1",shu.get("zhuzhenyishimingcheng_2"));
            map.put("liyuanfangshi_7",empty);
            map.put("chongxiaodanjuhao_3",empty);
            map.put("yidijiuzhenbiaozhi_1",empty);

            insertList.add(map);

            if(insertList.size()>=1500){
                mongoTemplate.insert(insertList,feiyongjiesuanku);
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
    void yishengbiaoclear(){
        String yishengbiao = "com.ns.entity.object.form.instance.nsjgpv79iu55j528ig78.yishengbiao_1";
        List<Map> all = mongoTemplate.findAll(Map.class,yishengbiao);
        if(all.size()>0){
            mongoTemplate.remove(new Query(),yishengbiao);
            // System.out.println(all);
        }
    }


    @Test
    void xx(){
        String str = "{unit=元, value=213}";
        Double x = 9.0;
        x += Double.parseDouble(mapStringToMap(str).get("value").toString());
        System.out.println(x);
    }

    @Test
    public  Map<String,Object> mapStringToMap(String str){
        str = str.substring(1, str.length()-1);
        String[] strs = str.split(",");
        Map<String,Object> map = new HashMap<String, Object>();
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
    void yishengbiaodaoru(){
        List<Map> jiesuan = new ArrayList<>();
        List<Map> mingxi = new ArrayList<>();



        String yishengbiaoku = "com.ns.entity.object.form.instance.nsjgpv79iu55j528ig78.yishengbiao_1";
        String jiesuanku = "com.ns.entity.object.form.instance.nsjgpv79iu55j528ig78.feiyongjiesuanxinxi_1";
        String mingxiku = "com.ns.entity.object.form.instance.nsjgpv79iu55j528ig78.feiyongmingxixinxi_1";

        //jiesuan = mongoTemplate.findAll(Map.class,jiesuanku);

        jiesuan = mongoTemplate.find(new Query(Criteria.where("zhuzhiyishengdaima_1").is("276")), Map.class,jiesuanku);



       // mingxi = mongoTemplate.findAll(Map.class,mingxiku);
      //  Map<String,List<Map<String,Object>>> mingxiyisheng = new HashMap<>();
      //  for (Map map:mingxi){
       //     String bianma = map.get("yishengbianma_5").toString();
       //     mingxiyisheng.
       // }


        List<Map<String, Object>> insertList = new ArrayList<>();

        for(Map jie:jiesuan){
            String daima = jie.get("zhuzhiyishengdaima_1").toString();
            if(!mongoTemplate.find(new Query(Criteria.where("yishengbianma_9").is(daima)),String.class,yishengbiaoku).isEmpty()){
                continue;
            }
            boolean tiao = true;
            for(Map t:insertList){
                if(daima.equals(t.get("yishengbianma_5"))){
                    tiao = false;
                    break;
                }
            }
            if(!tiao){
                continue;
            }

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



            List<Map> jieyisheng = mongoTemplate.find(new Query(Criteria.where("yishengbianma_9").is(daima)), Map.class,jiesuanku);
            List<Map> mingxiyisheng = mongoTemplate.find(new Query(Criteria.where("yishengbianma_5").is(daima)), Map.class,mingxiku);
            //业务字段
            map.put("yiyuanmingcheng_8",jie.get("dingdianjigoumingcheng_5"));
            map.put("keshimingcheng_30",jie.get("chuyuankeshimingcheng_2"));
            map.put("yishengmingcheng_1",jie.get("zhuzhiyisheng_1"));
            map.put("yibaoniandu_8","");
            map.put("yiyuanleibie_6","公立医院");
            map.put("yiyuandengji_6","三级");
            map.put("yiyuanxingzhi_6","");

            Map<String, Object> yibaoMap = new HashMap<>();
            yibaoMap.put("unit", "元");

            Double yibaomoney = 0.0;
            for (Map money:jieyisheng){
                yibaomoney +=  Double.parseDouble(mapStringToMap(money.get("tongchouzhifujine_5").toString()).get("value").toString());
            }

            yibaoMap.put("value",yibaomoney);
            map.put("yibaofanweifeiyong_7",yibaoMap);

            ArrayList<String> jiancha = new ArrayList<>();
            Double jianchafei = 0.0;
            int jianchacishu = 0;
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

            for (Map ming:mingxiyisheng){
                if(jiancha.contains(ming.get("feiyongleibie_11").toString())){
                    jianchafei += Double.parseDouble(mapStringToMap(ming.get("jine_20").toString()).get("value").toString());
                    jianchacishu ++;
                }
            }
            jianchaMap.put("value",jianchafei);

            map.put("jianchajianyanqiuhe_1",jianchaMap);

            jianchacMap.put("value",jianchacishu);
            map.put("jianchajianyanjici_1",jianchacMap);

            int m =0,z=0;
            for(Map leibei:jieyisheng){
                if(leibei.get("yiliaoleibie_8").equals("普通门诊"))   m = 1;
                if(leibei.get("yiliaoleibie_8").equals("住院"))   z = 1;
                if(m+z==2) break;
            }
            map.put("yiliaoleibiejici_1",""+m+z);

            map.put("chuangweishu_1",0);

            Map<String, Object> CTMap = new HashMap<>();
            CTMap.put("unit", "元");
            Map<String, Object> CTcMap = new HashMap<>();
            CTcMap.put("unit", "次");
            Double CTMoney = 0.0;
            int CTCount = 0;
            for (Map CT:mingxiyisheng){
                if(CT.get("shoufeixiangmuleibie_5").equals("CT费")){
                    CTMoney +=Double.parseDouble(mapStringToMap(CT.get("jine_20").toString()).get("value").toString());
                    CTCount++;
                }
            }
            CTMap.put("value",CTMoney);
            map.put("CTqiuhe_1",CTMap);
            CTcMap.put("value",CTCount);
            map.put("CTjici_1",CTcMap);

            Map<String, Object> TCDMap = new HashMap<>();
            TCDMap.put("unit", "元");

            Map<String, Object> TCDcMap = new HashMap<>();
            TCDcMap.put("unit", "次");

            Double TCDMoney = 0.0;
            int TCDCount = 0;
            for (Map TCD:mingxiyisheng){
                if(TCD.get("shoufeixiangmuleibie_5").equals("TCD")){
                    TCDMoney += Double.parseDouble(mapStringToMap(TCD.get("jine_20").toString()).get("value").toString());
                    TCDCount++;
                }
            }
            TCDMap.put("value",TCDMoney);
            map.put("TCDqiuhe_1",TCDMap);
            TCDcMap.put("value",TCDCount);
            map.put("TCDjici_1",TCDcMap);

            Map<String, Object> bingliMap = new HashMap<>();
            bingliMap.put("unit", "元");

            Map<String, Object> bcMap = new HashMap<>();
            bcMap.put("unit", "次");
            Double bingliMoney = 0.0;
            int bingliCount = 0;
            for (Map bingli:mingxiyisheng){
                if(bingli.get("shoufeixiangmuleibie_5").equals("病理费")){
                    bingliMoney += Double.parseDouble(mapStringToMap(bingli.get("jine_20").toString()).get("value").toString());
                    bingliCount++;
                }
            }
            bingliMap.put("value",bingliMoney);
            map.put("bingliqiuhe_1",bingliMap);
            bcMap.put("value",bingliCount);
            map.put("binglijici_1",bcMap);

            Map<String, Object> cailiaoMap = new HashMap<>();
            cailiaoMap.put("unit", "元");
            Map<String, Object> caicMap = new HashMap<>();
            caicMap.put("unit", "次");

            Double cailiaoMoney = 0.0;
            int cailiaoCount = 0;
            for (Map cailiao:mingxiyisheng){
                if(cailiao.get("shoufeixiangmuleibie_5").equals("材料费")){
                    cailiaoMoney += Double.parseDouble(mapStringToMap(cailiao.get("jine_20").toString()).get("value").toString());
                    cailiaoCount++;
                }
            }
            cailiaoMap.put("value",cailiaoMoney);
            map.put("cailiaoqiuhe_1",cailiaoMap);
            caicMap.put("value",cailiaoCount);
            map.put("cailiaojici_1",caicMap);

            Map<String, Object> caichaoMap = new HashMap<>();
            caichaoMap.put("unit", "元");
            Map<String, Object> cailcMap = new HashMap<>();
            cailcMap.put("unit", "次");
            Double caichaoMoney = 0.0;
            int caichaoCount = 0;
            for (Map caichao:mingxiyisheng){
                if(caichao.get("shoufeixiangmuleibie_5").equals("彩超费")){
                    cailiaoMoney += Double.parseDouble(mapStringToMap(caichao.get("jine_20").toString()).get("value").toString());
                    cailiaoCount++;
                }
            }
            caichaoMap.put("value",caichaoMoney);
            map.put("caichaoqiuhe_1",caichaoMap);
            cailcMap.put("value",cailcMap);
            map.put("caichaojici_1 ",caichaoCount);


            Map<String, Object> caoyaoMap = new HashMap<>();
            caoyaoMap.put("unit", "元");
            Map<String, Object> caocMap = new HashMap<>();
            caocMap.put("unit", "次");
            Double caoyaoMoney = 0.0;
            int caoyaoCount = 0;
            for (Map caoyao:mingxiyisheng){
                if(caoyao.get("shoufeixiangmuleibie_5").equals("草药费")){
                    caoyaoMoney += Double.parseDouble(mapStringToMap(caoyao.get("jine_20").toString()).get("value").toString());
                    caoyaoCount++;
                }
            }
            caoyaoMap.put("value",caoyaoMoney);
            map.put("caoyaoqiuhe_1",caoyaoMap);
            caocMap.put("value",caoyaoCount);
            map.put("caoyaojici_1",caocMap);


            Map<String, Object> zhongchengMap = new HashMap<>();
            zhongchengMap.put("unit", "元");
            Map<String, Object> zcMap = new HashMap<>();
            zcMap.put("unit", "次");
            Double zhongchengyaoMoney = 0.0;
            int zhongchengyaoCount = 0;
            for (Map zhong:mingxiyisheng){
                if(zhong.get("shoufeixiangmuleibie_5").equals("中成药费")){
                    zhongchengyaoMoney += Double.parseDouble(mapStringToMap(zhong.get("jine_20").toString()).get("value").toString());
                    zhongchengyaoCount++;
                }
            }
            zhongchengMap.put("value",zhongchengyaoMoney);
            map.put("zhongchengyaoqiuhe_1",zhongchengMap);
            zcMap.put("value",zhongchengyaoCount);
            map.put("zhongchengyaojici_1",zcMap);


            Map<String, Object> ciMap = new HashMap<>();
            ciMap.put("unit", "元");
            Map<String, Object> cicMap = new HashMap<>();
            cicMap.put("unit", "次");
            Double cigongzhenMoney = 0.0;
            int cigongzhenCount = 0;
            for (Map ci:mingxiyisheng){
                if(ci.get("shoufeixiangmuleibie_5").equals("磁共振")){
                    cigongzhenMoney += Double.parseDouble(mapStringToMap(ci.get("jine_20").toString()).get("value").toString());
                    cigongzhenCount++;
                }
            }
            ciMap.put("value",cigongzhenMoney);
            map.put("cigongzhenqiuhe_1",ciMap);
            cicMap.put("value",cigongzhenCount);
            map.put("cigongzhenjici_1",cicMap);


            Map<String, Object> huMap = new HashMap<>();
            huMap.put("unit", "元");
            Map<String, Object> hucMap = new HashMap<>();
            hucMap.put("unit", "次");
            Double huliMoney = 0.0;
            int huliCount = 0;
            for (Map hu:mingxiyisheng){
                if(hu.get("shoufeixiangmuleibie_5").equals("护理费")){
                    huliMoney += Double.parseDouble(mapStringToMap(hu.get("jine_20").toString()).get("value").toString());
                    huliCount++;
                }
            }

            huMap.put("value",huliMoney);
            map.put("huliqiuhe_1",huMap);
            hucMap.put("value",huliCount);
            map.put("hulijici_1",hucMap);

            Map<String, Object> huaMap = new HashMap<>();
            huaMap.put("unit", "元");
            Map<String, Object> huacMap = new HashMap<>();
            huacMap.put("unit", "次");
            Double huayanMoney = 0.0;
            int huayanCount = 0;
            for (Map hua:mingxiyisheng){
                if(hua.get("shoufeixiangmuleibie_5").equals("化验费")){
                    huayanMoney += Double.parseDouble(mapStringToMap(hua.get("jine_20").toString()).get("value").toString());
                    huayanCount++;
                }
            }
            huaMap.put("value",huayanMoney);
            map.put("huayanqiuhe_1",huaMap);
            huacMap.put("value",huayanCount);
            map.put("huayanjici_1",huacMap);

            Map<String, Object> huanMap = new HashMap<>();
            huaMap.put("unit", "元");
            Map<String, Object> huancMap = new HashMap<>();
            huacMap.put("unit", "次");
            Double huanyaoMoney = 0.0;
            int huanyaoCount = 0;
            for (Map huan:mingxiyisheng){
                if(huan.get("shoufeixiangmuleibie_5").equals("换药费")){
                    huayanMoney += Double.parseDouble(mapStringToMap(huan.get("jine_20").toString()).get("value").toString());
                    huayanCount++;
                }
            }
            huanMap.put("value",huanyaoMoney);
            map.put("huanyaoqiuhe_1",huanMap);
            huancMap.put("value",huanyaoCount);
            map.put("huanyaojici_1",huancMap);

            Map<String, Object> jianMap = new HashMap<>();
            jianMap.put("unit", "元");
            Map<String, Object> jiancMap = new HashMap<>();
            jiancMap.put("unit", "次");
            Double jianchaMoney = 0.0;
            int jianchaCount = 0;
            for (Map jian:mingxiyisheng){
                if(jian.get("shoufeixiangmuleibie_5").equals("检查费")){
                    jianchaMoney += Double.parseDouble(mapStringToMap(jian.get("jine_20").toString()).get("value").toString());
                    jianchaCount ++;
                }
            }
            jiancMap.put("value",jianchaMoney);
            map.put("jianchaqiuhe_1",jianMap);
            jiancMap.put("value",jianchaCount);
            map.put("jianchajici_1",jiancMap);

            Map<String, Object> jiuMap = new HashMap<>();
            jiuMap.put("unit", "元");
            Map<String, Object> jiucMap = new HashMap<>();
            jiucMap.put("unit", "次");
            Double jiuhucheMoney = 0.0;
            int jiuhucheCount = 0;
            for (Map jiu:mingxiyisheng){
                if(jiu.get("shoufeixiangmuleibie_5").equals("救护车费")){
                    jiuhucheMoney += Double.parseDouble(mapStringToMap(jiu.get("jine_20").toString()).get("value").toString());
                    jiuhucheCount ++;
                }
            }
            jiuMap.put("value",jiuhucheMoney);
            map.put("jiuhucheqiuhe_1",jiuMap);
            jiucMap.put("value",jiuhucheCount);
            map.put("jiuhuchejici_1",jiucMap);

            Double liliaoMoney = 0.0;
            int liliaoCount = 0;
            for (Map liliao:mingxiyisheng){
                if(liliao.get("shoufeixiangmuleibie_5").equals("理疗费")){
                    liliaoMoney += Double.parseDouble(mapStringToMap(liliao.get("jine_20").toString()).get("value").toString());
                    liliaoCount ++;
                }
            }
            map.put("liliaoqiuhe_1",liliaoMoney);
            map.put("liliaojici_1",liliaoCount);

            Double mazuiMoney = 0.0;
            int mazuiCount = 0;
            for (Map ma:mingxiyisheng){
                if(ma.get("shoufeixiangmuleibie_5").equals("麻醉费")){
                    mazuiMoney += Double.parseDouble(mapStringToMap(ma.get("jine_20").toString()).get("value").toString());
                    mazuiCount ++;
                }
            }
            map.put("mazuiqiuhe_1",mazuiMoney);
            map.put("mazuijici_1",mazuiCount);

            Double naodianMoney = 0.0;
            int naodianCount = 0;
            for (Map nao:mingxiyisheng){
                if(nao.get("shoufeixiangmuleibie_5").equals("脑电图")){
                    naodianMoney += Double.parseDouble(mapStringToMap(nao.get("jine_20").toString()).get("value").toString());
                    naodianCount ++;
                }
            }
            map.put("naodiantuqiuhe_1",naodianMoney);
            map.put("naodiantujici_1",naodianCount);

            Double paipianMoney = 0.0;
            int paipianCount = 0;
            for (Map pai:mingxiyisheng){
                if(pai.get("shoufeixiangmuleibie_5").equals("拍片费")){
                    paipianMoney +=Double.parseDouble(mapStringToMap(pai.get("jine_20").toString()).get("value").toString());
                    paipianCount ++;
                }
            }
            map.put("paipianqiuhe_1",paipianMoney);
            map.put("paipianjici_1",paipianCount);

            Double shoushuMoney = 0.0;
            int shoushuCount = 0;
            for (Map shou:mingxiyisheng){
                if(shou.get("shoufeixiangmuleibie_5").equals("手术材料费")){
                    shoushuMoney += Double.parseDouble(mapStringToMap(shou.get("jine_20").toString()).get("value").toString());
                    shoushuCount ++;
                }
            }
            map.put("shoushucailiaoqiuhe_1",shoushuMoney);
            map.put("shoushucailiaojici_1",shoushuCount);

            Double shouMoney = 0.0;
            int shouCount = 0;
            for (Map s:mingxiyisheng){
                if(s.get("shoufeixiangmuleibie_5").equals("手术费")){
                    shouMoney += Double.parseDouble(mapStringToMap(s.get("jine_20").toString()).get("value").toString());
                    shouCount ++;
                }
            }
            map.put("shoushuqiuhe_1",shouMoney);
            map.put("shoushujici_1",shouCount);

            Double tijianMoney = 0.0;
            int tijianCount = 0;
            for (Map t:mingxiyisheng){
                if(t.get("shoufeixiangmuleibie_5").equals("体检费")){
                    tijianMoney += Double.parseDouble(mapStringToMap(t.get("jine_20").toString()).get("value").toString());
                    tijianCount ++;
                }
            }
            map.put("tijianqiuhe_1",tijianMoney);
            map.put("tijianjici_1",tijianCount);


            Double weijingMoney = 0.0;
            int weijingCount = 0;
            for (Map w:mingxiyisheng){
                if(w.get("shoufeixiangmuleibie_5").equals("胃镜费")){
                    weijingMoney += Double.parseDouble(mapStringToMap(w.get("jine_20").toString()).get("value").toString());
                    weijingCount ++;
                }
            }
            map.put("weijingqiuhe_1",weijingMoney);
            map.put("weijingjici_1",weijingCount);

            Double xiyaoMoney = 0.0;
            int xiyaoCount = 0;
            for (Map x:mingxiyisheng){
                if(x.get("shoufeixiangmuleibie_5").equals("西药费")){
                    xiyaoMoney += Double.parseDouble(mapStringToMap(x.get("jine_20").toString()).get("value").toString());
                    xiyaoCount ++;
                }
            }
            map.put("xiyaoqiuhe_1",xiyaoMoney);
            map.put("xiyaojici_1",xiyaoCount);

            Double xinchaoMoney = 0.0;
            int xinchaoCount = 0;
            for (Map x:mingxiyisheng){
                if(x.get("shoufeixiangmuleibie_5").equals("心超费")){
                    xinchaoMoney += Double.parseDouble(mapStringToMap(x.get("jine_20").toString()).get("value").toString());
                    xinchaoCount ++;
                }
            }

            map.put("xinchaoqiuhe_1",xinchaoMoney);
            map.put("xinchaojici_1",xinchaoCount);

            Double zhenliaoMoney = 0.0;
            int zhenliaoCount = 0;
            for (Map x:mingxiyisheng){
                if(x.get("shoufeixiangmuleibie_5").equals("诊疗费")){
                    zhenliaoMoney += Double.parseDouble(mapStringToMap(x.get("jine_20").toString()).get("value").toString());
                    zhenliaoCount ++;
                }
            }

            map.put("zhenliaoqiuhe_1",zhenliaoMoney);
            map.put("zhenliaojici_1",zhenliaoCount);

            Double zhiliaoMoney = 0.0;
            int zhiliaoCount = 0;
            for (Map x:mingxiyisheng){
                if(x.get("shoufeixiangmuleibie_5").equals("治疗费")){
                    zhiliaoMoney += Double.parseDouble(mapStringToMap(x.get("jine_20").toString()).get("value").toString());
                    zhiliaoCount ++;
                }
            }

            map.put("zhiliaoqiuhe_1",zhiliaoMoney);
            map.put("zhiliaojici_1",zhiliaoCount);

            Double zhusheMoney = 0.0;
            int zhusheCount = 0;
            for (Map x:mingxiyisheng){
                if(x.get("shoufeixiangmuleibie_5").equals("注射费")){
                    zhusheMoney += Double.parseDouble(mapStringToMap(x.get("jine_20").toString()).get("value").toString());
                    zhusheCount ++;
                }
            }
            map.put("zhusheqiuhe_1",zhusheMoney);
            map.put("zhushejici_1",zhusheCount);


            Double zongjiner = 0.0;
            for (Map x:jieyisheng){
                zongjiner += Double.parseDouble(mapStringToMap(x.get("yiliaofeizonge_6").toString()).get("value").toString());
            }
            map.put("danweishijianzongjine_1",zongjiner);

            Double yibaozong = 0.0;
            for (Map x:jieyisheng){
                yibaozong += Double.parseDouble(mapStringToMap(x.get("tongchouzhifujine_5").toString()).get("value").toString());
            }
            map.put("danweishijianyibaobaoxiaozonge_1",yibaozong);

            Double bili = yibaozong/zongjiner;
            map.put("danweishijianyibaobili_1",bili);

            Map<Object,Object> quchong = new HashMap<>();
            for (Map x:mingxiyisheng){
                quchong.put(x.get("keshimingcheng_26"),"1");
            }
            map.put("danweishijiankeshishuliang_1",quchong.size());

            map.put("danweishijianyishengshuliang_1",1);
            map.put("danweishijianchuangweishu_1",0);
            map.put("danweishijianbendiyidibili_1",0.0);

            Map<Object,Object> jiezhen = new HashMap<>();
            for (Map x:jieyisheng){
                jiezhen.put(x.get("xingming_103"),"1");
            }
            map.put("danweishijianneijiezhenrenci_1",jiezhen.size());

            Map<Object,Object> zhuyuan = new HashMap<>();
            for (Map x:jieyisheng){
                if(x.get("yiliaoleibie_8").equals("住院")){
                    zhuyuan.put(x.get("xingming_103"),"1");
                }
            }
            map.put("danweishijianzhuyuanrenci_1",zhuyuan.size());

            Map<Object,Object> shoushu = new HashMap<>();
            for (Map x:mingxiyisheng){
                if(x.get("shoufeixiangmuleibie_5").toString().contains("手术")){
                    shoushu.put(x.get("renyuanbianhao_3"),"1");
                }
            }
            map.put("danweishijianshoushurenci_1",shoushu.size());

            map.put("danweishijiankoufuyaozhonglei_1","0");
            insertList.add(map);

            if(insertList.size()>=1500){
                mongoTemplate.insert(insertList,yishengbiaoku);
                insertList = new ArrayList<>();
                //   System.out.println(map);
            }
        }
        if (insertList.size() > 0) {
            mongoTemplate.insert(insertList, yishengbiaoku);
            insertList = new ArrayList<>();
        }
    }


}
