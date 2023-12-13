package com.example.controller;

import com.example.service.ImportService;
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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.*;

import java.io.*;
import java.util.*;

import static com.mongodb.WriteConcern.ACKNOWLEDGED;
import static java.lang.Math.floor;

@RequestMapping("import")
@RestController
public class ImportController {
    //多线程
    @Autowired
    private ImportService importService;

    /**
     * 原始三张表
     *
     * @param collectionName 在mongo中的表全称 例：com.ns.entity.object.form.instance.ns3g475kl6jj2eb4ixfi.yibaozhenduanxinxi_1
     * @param target         要导入的表 zhenduan:医保_诊断信息 jiesuan: 医保_费用结算信息 mingxi:医保_费用明细信息
     * @param filepath       csv文件路径 中文需编码处理
     */
    @GetMapping("csv")
    public String importsDataOpt(@RequestParam(required = true, defaultValue = "") String collectionName,
                               @RequestParam(required = true, defaultValue = "") String target,
                               @RequestParam(required = true, defaultValue = "") String filepath) {
       return importService.importsDataOpt(collectionName, target,filepath);
    }

    @GetMapping("datagovern")
     private String DataGovernance(@RequestParam(required = true, defaultValue = "") String target,
                                   @RequestParam(required = true, defaultValue = "") String collectionName,
                                   @RequestParam(required = true, defaultValue = "") int theadnum
                                ) {
        return importService.gapControl(target,collectionName,theadnum);
    }
}

