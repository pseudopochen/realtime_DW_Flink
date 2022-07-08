package com.luban.micromalllogger.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;

@RestController
@Slf4j
public class LoggerController {

    @Resource
    KafkaTemplate<String, String> kafkaTemplate;

    @GetMapping("test")
    String test1() {
        System.out.println("Success");
        return "Hello World!";
    }

    @GetMapping("test2")
    String test2(@RequestParam("name") String nn,
                 @RequestParam(value = "age", defaultValue = "18") Integer age) {
        System.out.println("Hello " + nn + " age " + age);
        return "success";
    }

    @GetMapping("applog")
    String getLog(@RequestParam("param") String jsonStr) {
        log.info(jsonStr);
        kafkaTemplate.send("ods_base_log", jsonStr);
        return "success";
    }

}
