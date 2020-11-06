package com.yaojun;

import com.alibaba.fastjson.JSON;
import com.yaojun.pojo.User;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

@SpringBootTest
class DemoApplicationTests {
    @Autowired
    @Qualifier("restHighLevelClient")   //配置类中的方法名
    private RestHighLevelClient client;

    //测试索引的创建
    @Test
    void testCreateIndex() throws IOException {
        //1.创建索引
        CreateIndexRequest request = new CreateIndexRequest("yaojun_index");
        //2.客户端执行请求 获得响应
        CreateIndexResponse response = client.indices().create(request, RequestOptions.DEFAULT);

        System.out.println(response);
    }
    //获取索引
    @Test
    void testGetIndex() throws IOException {
        //1.获取索引
        GetIndexRequest request = new GetIndexRequest("yaojun_index");
        //2.客户端执行请求 获得响应
        boolean response = client.indices().exists(request, RequestOptions.DEFAULT);
        System.out.println(response);
    }
    //删除索引，注意只能删除一次
    @Test
    void testDeleteIndex() throws IOException {
        DeleteIndexRequest request = new DeleteIndexRequest("yaojun_index");
        AcknowledgedResponse response = client.indices().delete(request, RequestOptions.DEFAULT);
        System.out.println(response.isAcknowledged());
    }

    //测试添加文档
    @Test
    void testAddDocument() throws Exception{
        //1.创建对象
        User user = new User("yaojun", 3);
        // 2.创建请求
        IndexRequest request = new IndexRequest("yaojun");
        //3.规则 put /yaojun_index/doc/1
        request.id("1");
        request.timeout(TimeValue.timeValueSeconds(1));
        //4.将我们的数据放入请求 json
        IndexRequest source = request.source(JSON.toJSONString(user), XContentType.JSON);

        //5.客户端发送请求 获取响应结果
        IndexResponse response = client.index(request, RequestOptions.DEFAULT);

        System.out.println(response.toString());
        System.out.println(response.status());
    }

    //获取文档 判断是否存在get /yaojun/doc/1
    @Test
    void testIsExist() throws Exception{
        GetRequest getRequest = new GetRequest("yaojun", "1");
        //不获取返回的_source的上下文了
        getRequest.fetchSourceContext(new FetchSourceContext(false));

        boolean exists = client.exists(getRequest, RequestOptions.DEFAULT);
        System.out.println(exists);
    }

    //获取文档
    @Test
    void testGetDocument()throws  Exception{
        GetRequest getRequest = new GetRequest("yaojun", "1");


        GetResponse response = client.get(getRequest, RequestOptions.DEFAULT);
        System.out.println(response.getSourceAsString()); //打印文档的内容
        System.out.println(response);
    }
    //更新文档
    @Test
    void testUpdateDocument()throws  Exception{
        UpdateRequest updateRequest = new UpdateRequest("yaojun", "1");
        updateRequest.timeout("1s");
        User user = new User("姚军",18);
        updateRequest.doc(JSON.toJSONString(user), XContentType.JSON);

        UpdateResponse response = client.update(updateRequest, RequestOptions.DEFAULT);
        System.out.println(response.status());
    }
    //删除文档
    @Test
    void testDeleteDocument()throws  Exception{
        DeleteRequest request = new DeleteRequest("yaojun", "1");
        request.timeout("1s");


        DeleteResponse response = client.delete(request, RequestOptions.DEFAULT);
        System.out.println(response.status());
    }

    //删除文档
    @Test
    void testBulkDocument()throws  Exception{
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.timeout("10s");

        ArrayList<User> userList = new ArrayList<>();
        userList.add(new User("姚军1", 18));
        userList.add(new User("姚军2", 19));
        userList.add(new User("姚军3", 8));
        userList.add(new User("姚军4", 8));
        userList.add(new User("姚军5", 8));

        //批处理请求
        for(int i = 0; i < userList.size(); i++){
            bulkRequest.add(
                    new IndexRequest("yaojun").id(""+(i+1)).source(JSON.toJSONString(userList.get(i)), XContentType.JSON)
            );
        }
        BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);

        System.out.println(bulkResponse.hasFailures()); //判断是否失败
    }

    //查询
    @Test
    void testSearchDocument()throws Exception{
        SearchRequest request = new SearchRequest("yaojun");
        //构建搜索条件
        SearchSourceBuilder searchSource = new SearchSourceBuilder();
        //查询语句
        //QueryBuilders.termQuery() 精确匹配
        //  QueryBuilders.matchAllQuery() 匹配全部

        TermQueryBuilder termQuery = QueryBuilders.termQuery("name", "yaojun");
        searchSource.query(termQuery);
        //searchSource.from(); 构建分页
        //searchSource.size();

        searchSource.timeout(new TimeValue(60, TimeUnit.SECONDS));

        request.source(searchSource);
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        System.out.println(JSON.toJSONString(response.getHits()));
        System.out.println("====================");
        for (SearchHit hit : response.getHits().getHits()) {
            System.out.println(hit.getSourceAsMap());
        }
    }
}
