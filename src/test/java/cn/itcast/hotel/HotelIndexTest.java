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
    * ????????????
    * */
    @Test
    void testMatchAll() throws IOException {
        //1.??????Request
        SearchRequest request = new SearchRequest("hotel");
        //2.??????DSL
        request.source()
                .query(QueryBuilders.matchAllQuery());
        //3.????????????
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        //4.1??????hits
        handleResponse(response);
    }


    /*
    * ??????????????????
    * */
    @Test
    void testMatch() throws IOException {
        //1.??????Request
        SearchRequest request = new SearchRequest("hotel");
        //2.??????DSL
        request.source()
                .query(QueryBuilders.matchQuery("name","??????"));
        //3.????????????
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        handleResponse(response);
    }

    /*
     * bool????????????
     * */
    @Test
    void testBool() throws IOException {
        //1.??????Request
        SearchRequest request = new SearchRequest("hotel");
        //2.??????DSL
        //2.1??????BoolQuery
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        //2.2??????term
        boolQuery.must(QueryBuilders.termQuery("city","??????"));
        //2.3??????range
        boolQuery.filter(QueryBuilders.rangeQuery("price").lte(250));
        request.source()
                .query(boolQuery);
        //3.????????????
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        handleResponse(response);
    }

    /*
     * ??????
     * */
    @Test
    void testPageAndSort() throws IOException {
        //1.??????Request
        SearchRequest request = new SearchRequest("hotel");
        //2.??????DSL
        //2.1query
        request.source().query(QueryBuilders.matchAllQuery());
        //2.2??????sort
        request.source().sort("price", SortOrder.ASC);
        //2.3?????? from???size
        request.source().from(0).size(5);
        //3.????????????
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        handleResponse(response);
    }

    /*
     * ??????
     * */
    @Test
    void testTestHighLight() throws IOException {
        //1.??????Request
        SearchRequest request = new SearchRequest("hotel");
        //2.??????DSL
        //2.1query
        request.source().query(QueryBuilders.matchQuery("all","??????"));
        //2.2??????
        request.source().highlighter(new HighlightBuilder().field("name").requireFieldMatch(false));

        //3.????????????
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        //4.????????????
        handleResponse(response);
    }

    /*
    * ??????????????????
    * */
    private void handleResponse(SearchResponse response) {
        //4.1??????hits
        SearchHits searchHits = response.getHits();
        //4.1 ???????????????
        long total = searchHits.getTotalHits().value;
        System.out.println("????????????"+total + "???");
        //4.2????????????
        SearchHit[] hits = searchHits.getHits();
        //4.3??????
        for (SearchHit hit:hits){
            //????????????source
            String json = hit.getSourceAsString();
            //????????????
            HotelDoc hotelDoc = JSON.parseObject(json, HotelDoc.class);
            //??????????????????
            Map<String, HighlightField> highlightFields = hit.getHighlightFields();
            if(!CollectionUtils.isEmpty(highlightFields)){
                //????????????????????????????????????
                HighlightField highlightField = highlightFields.get("name");
                if(highlightField != null){
                    //???????????????
                    String name = highlightField.getFragments()[0].string();
                    //?????????????????????
                    hotelDoc.setName(name);
                }
            }
            System.out.println(hotelDoc);
        }
    }

    /*
     * ????????????
     * */
    @Test
    void testAddDocument() throws IOException {

        //??????id??????????????????
        Hotel hotel = hotelService.getById(61083);
        //?????????????????????
        HotelDoc hotelDoc = new HotelDoc(hotel);
        //1.??????Request??????
        IndexRequest request = new IndexRequest("hotel").id(hotelDoc.getId().toString());
        //2.??????JSON??????
        request.source(JSON.toJSONString(hotelDoc),XContentType.JSON);
        //3.????????????
        client.index(request,RequestOptions.DEFAULT);
    }

    /*
    * ????????????
    * */
    @Test
    void testGetDocumentById() throws IOException {
        //1.??????Request
        GetRequest request = new GetRequest("hotel", "61083");
        //2.???????????????????????????
        GetResponse response = client.get(request, RequestOptions.DEFAULT);
        //3.??????????????????
        String json = response.getSourceAsString();

        HotelDoc hotelDoc = JSON.parseObject(json, HotelDoc.class);
        System.out.println(hotelDoc);
    }

    /*
    *????????????
    * */
    @Test
    void testUpdateDocuemtn() throws IOException {
        //1.??????request
        UpdateRequest request = new UpdateRequest("hotel", "61083");
        //2.??????????????????
        request.doc(
                "price","952",
                "starName","??????"
        );
        //3.????????????
        client.update(request,RequestOptions.DEFAULT);
    }

    /*
    * ????????????
    * */
    @Test
    void testDeleteDocument() throws IOException {
        //1.??????Request
        DeleteRequest request = new DeleteRequest("hotel", "61093");
        //2????????????
        client.delete(request,RequestOptions.DEFAULT);
    }

    /*
    * ??????????????????
    * */
    @Test
    void testBulkRequest() throws IOException {
        //??????????????????
        List<Hotel> hotels = hotelService.list();
        //?????????????????????HotelDoc


        //1.??????Request
        BulkRequest request = new BulkRequest();
        //2.???????????????????????????
        for(Hotel hotel: hotels){
            HotelDoc hotelDoc = new HotelDoc(hotel);
            request.add(new IndexRequest("hotel")
                    .id(hotelDoc.getId()
                            .toString())
                    .source(JSON.toJSONString(hotelDoc),XContentType.JSON));
        }
        //3.????????????
        client.bulk(request,RequestOptions.DEFAULT);
    }

    @Test
    void createHotelIndex() throws IOException {
        //1.??????Request??????
        CreateIndexRequest request = new CreateIndexRequest("hotel");
        //2.????????????????????????DSL??????
        request.source(MAPPING_TEMPLATE, XContentType.JSON);
        //3.????????????
        client.indices().create(request, RequestOptions.DEFAULT);
    }

    @Test
    void testDeleteHotelIndex() throws IOException {
        //1.??????Request??????
        DeleteIndexRequest request = new DeleteIndexRequest("hotel");
        //2.????????????
        client.indices().delete(request, RequestOptions.DEFAULT);
    }

    @Test
    void testExistHotelIndex() throws IOException {
        //1.??????Request??????
        GetIndexRequest request = new GetIndexRequest("hotel");
        //2.????????????
        boolean exists = client.indices().exists(request, RequestOptions.DEFAULT);
        //3.??????
        System.out.println(exists ? "?????????????????????" : "?????????????????????");
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
