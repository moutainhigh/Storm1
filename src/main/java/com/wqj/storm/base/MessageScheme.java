package com.wqj.storm.base;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.io.UnsupportedEncodingException;
import java.util.List;

/**
 * @Auther: wqj
 * @Date: 2018/6/2 19:09
 * @Description:
 */
public class MessageScheme implements Scheme {
    public List<Object> deserialize(byte[] bytes) {
        String msg = null;
        try {
            msg = new String(bytes,"UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return new Values(msg);
    }

    public Fields getOutputFields() {
        return new Fields("msg");
    }
}
