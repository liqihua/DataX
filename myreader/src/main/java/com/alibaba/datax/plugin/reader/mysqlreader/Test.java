package com.alibaba.datax.plugin.reader.mysqlreader;

import net.sf.json.JSONArray;
import org.apache.ibatis.io.Resources;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

/**
 * @author liqihua
 * @since 2018/6/22
 */
public class Test {

    @org.junit.Test
    public void test1() throws IOException {
        /**
         *  1、获得 SqlSessionFactory
         *  2、获得 SqlSession
         *  3、调用在 mapper 文件中配置的 SQL 语句
         */
        String resource = "mybatis.xml";           // 定位核心配置文件
        InputStream inputStream = Resources.getResourceAsStream(resource);
        SqlSessionFactory sqlSessionFactory = new SqlSessionFactoryBuilder().build(inputStream);    // 创建 SqlSessionFactory

        SqlSession sqlSession = sqlSessionFactory.openSession();    // 获取到 SqlSession

        // 调用 mapper 中的方法：命名空间 + id
        List<Map<String,Object>> list = sqlSession.selectList("com.alibaba.datax.plugin.reader.mysqlreader.TestDao.test1");

        System.out.println("--- list.size : "+list.size());
        System.out.println(JSONArray.fromObject(list));


    }

}
