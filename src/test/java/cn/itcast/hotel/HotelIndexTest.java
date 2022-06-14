package cn.itcast.hotel;

import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.pojo.HotelDoc;
import cn.itcast.hotel.service.IHotelService;
import com.alibaba.fastjson.JSON;
import com.baomidou.mybatisplus.extension.api.R;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.util.CollectionUtils;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static cn.itcast.hotel.constants.HotelConstants.MAPPING_TEMPLATE;

@SpringBootTest
public class HotelIndexTest {

    @Autowired
    private IHotelService hotelService;

    private RestHighLevelClient client;

    @Test
    void testInit() {
        System.out.println(client);
    }


    /*
    * 全文搜索
    * */
    @Test
    void testMatchAll() throws IOException {
        //1.准备Request
        SearchRequest request = new SearchRequest("hotel");
        //2.准备DSL
        request.source()
                .query(QueryBuilders.matchAllQuery());
        //3.发送请求
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        //4.1解析hits
        handleResponse(response);
    }


    /*
    * 全文搜索查询
    * */
    @Test
    void testMatch() throws IOException {
        //1.准备Request
        SearchRequest request = new SearchRequest("hotel");
        //2.准备DSL
        request.source()
                .query(QueryBuilders.matchQuery("name","如家"));
        //3.发送请求
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        handleResponse(response);
    }

    /*
     * bool搜索查询
     * */
    @Test
    void testBool() throws IOException {
        //1.准备Request
        SearchRequest request = new SearchRequest("hotel");
        //2.准备DSL
        //2.1准备BoolQuery
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        //2.2添加term
        boolQuery.must(QueryBuilders.termQuery("city","武汉"));
        //2.3添加range
        boolQuery.filter(QueryBuilders.rangeQuery("price").lte(250));
        request.source()
                .query(boolQuery);
        //3.发送请求
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        handleResponse(response);
    }

    /*
     * 分页
     * */
    @Test
    void testPageAndSort() throws IOException {
        //1.准备Request
        SearchRequest request = new SearchRequest("hotel");
        //2.准备DSL
        //2.1query
        request.source().query(QueryBuilders.matchAllQuery());
        //2.2排序sort
        request.source().sort("price", SortOrder.ASC);
        //2.3分页 from、size
        request.source().from(0).size(5);
        //3.发送请求
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        handleResponse(response);
    }

    /*
     * 高亮
     * */
    @Test
    void testTestHighLight() throws IOException {
        //1.准备Request
        SearchRequest request = new SearchRequest("hotel");
        //2.准备DSL
        //2.1query
        request.source().query(QueryBuilders.matchQuery("all","如家"));
        //2.2高亮
        request.source().highlighter(new HighlightBuilder().field("name").requireFieldMatch(false));

        //3.发送请求
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        //4.解析响应
        handleResponse(response);
    }

    /*
    * 抽离解析方法
    * */
    private void handleResponse(SearchResponse response) {
        //4.1解析hits
        SearchHits searchHits = response.getHits();
        //4.1 获取总条数
        long total = searchHits.getTotalHits().value;
        System.out.println("一共有："+total + "条");
        //4.2文档数组
        SearchHit[] hits = searchHits.getHits();
        //4.3遍历
        for (SearchHit hit:hits){
            //获取文档source
            String json = hit.getSourceAsString();
            //反序列化
            HotelDoc hotelDoc = JSON.parseObject(json, HotelDoc.class);
            //获取高亮结果
            Map<String, HighlightField> highlightFields = hit.getHighlightFields();
            if(!CollectionUtils.isEmpty(highlightFields)){
                //根据字段名称获取高亮结果
                HighlightField highlightField = highlightFields.get("name");
                if(highlightField != null){
                    //获取高亮值
                    String name = highlightField.getFragments()[0].string();
                    //覆盖非高亮结果
                    hotelDoc.setName(name);
                }
            }
            System.out.println(hotelDoc);
        }
    }

    /*
     * 插入文档
     * */
    @Test
    void testAddDocument() throws IOException {

        //根据id查询酒店数据
        Hotel hotel = hotelService.getById(61083);
        //转换为文档类型
        HotelDoc hotelDoc = new HotelDoc(hotel);
        //1.准备Request对象
        IndexRequest request = new IndexRequest("hotel").id(hotelDoc.getId().toString());
        //2.准备JSON文档
        request.source(JSON.toJSONString(hotelDoc),XContentType.JSON);
        //3.发送请求
        client.index(request,RequestOptions.DEFAULT);
    }

    /*
    * 查询文档
    * */
    @Test
    void testGetDocumentById() throws IOException {
        //1.准备Request
        GetRequest request = new GetRequest("hotel", "61083");
        //2.发送请求，得到响应
        GetResponse response = client.get(request, RequestOptions.DEFAULT);
        //3.解析响应结果
        String json = response.getSourceAsString();

        HotelDoc hotelDoc = JSON.parseObject(json, HotelDoc.class);
        System.out.println(hotelDoc);
    }

    /*
    *修改文档
    * */
    @Test
    void testUpdateDocuemtn() throws IOException {
        //1.准备request
        UpdateRequest request = new UpdateRequest("hotel", "61083");
        //2.准备请求参数
        request.doc(
                "price","952",
                "starName","四钻"
        );
        //3.发送请求
        client.update(request,RequestOptions.DEFAULT);
    }

    /*
    * 删除文档
    * */
    @Test
    void testDeleteDocument() throws IOException {
        //1.准备Request
        DeleteRequest request = new DeleteRequest("hotel", "61093");
        //2发送请求
        client.delete(request,RequestOptions.DEFAULT);
    }

    /*
    * 批量插入数据
    * */
    @Test
    void testBulkRequest() throws IOException {
        //批量查询酒店
        List<Hotel> hotels = hotelService.list();
        //转换为文档类型HotelDoc


        //1.准备Request
        BulkRequest request = new BulkRequest();
        //2.准备参数，添加多个
        for(Hotel hotel: hotels){
            HotelDoc hotelDoc = new HotelDoc(hotel);
            request.add(new IndexRequest("hotel")
                    .id(hotelDoc.getId()
                            .toString())
                    .source(JSON.toJSONString(hotelDoc),XContentType.JSON));
        }
        //3.发送请求
        client.bulk(request,RequestOptions.DEFAULT);
    }

    @Test
    void createHotelIndex() throws IOException {
        //1.创建Request对象
        CreateIndexRequest request = new CreateIndexRequest("hotel");
        //2.准备请求的参数：DSL语句
        request.source(MAPPING_TEMPLATE, XContentType.JSON);
        //3.发送请求
        client.indices().create(request, RequestOptions.DEFAULT);
    }

    @Test
    void testDeleteHotelIndex() throws IOException {
        //1.创建Request对象
        DeleteIndexRequest request = new DeleteIndexRequest("hotel");
        //2.发送请求
        client.indices().delete(request, RequestOptions.DEFAULT);
    }

    @Test
    void testExistHotelIndex() throws IOException {
        //1.创建Request对象
        GetIndexRequest request = new GetIndexRequest("hotel");
        //2.发送请求
        boolean exists = client.indices().exists(request, RequestOptions.DEFAULT);
        //3.所处
        System.out.println(exists ? "索引库已存在！" : "索引库不存在！");
    }

    @BeforeEach
    void setUp() {
        this.client = new RestHighLevelClient(RestClient.builder(
                HttpHost.create("http://192.168.189.128:9200")
        ));
    }

    @AfterEach
    void tearDown() throws IOException {
        this.client.close();
    }


}
