package cn.cmvideo.migu.usertags.tag;

import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.*;

/**
 * Created by Smart on 2017/6/26.
 */
public class TagBean {
    public String phone;
    public Map<String, Double> imei = new HashMap<>();
    public Map<String, Double> location = new HashMap<>();
    public Map<String, Double> class1 = new HashMap<>();
    public Map<String, Double> tag1 = new HashMap<>();
    public Map<String, Double> keyword = new HashMap<>();

    public void setPhone(String phone) {
        this.phone = phone;
    }


    private static void mapValueAdding(Map<String, Double> map, String key) {
        if (map.containsKey(key)) {
            double value = map.get(key);
            map.put(key, value + 1);
        } else {
            map.put(key, 1d);
        }
    }

    private static void mapValueMerging(Map<String, Double> map1, Map<String, Double> map2) {
        for(Map.Entry<String, Double> entry : map2.entrySet()) {
            if(map1.containsKey(entry.getKey())){
                String key = entry.getKey();
                double value = map1.get(key);
                map1.put(key, value + entry.getValue());
            } else {
                map1.put(entry.getKey(), entry.getValue());
            }
        }
    }

    public void imeiCountAddming(String key) {
        if(key.equals("-998")) {
            return;
        }
        mapValueAdding(this.imei, key);
    }

    public void locationCountAddming(String key) {
        mapValueAdding(this.location, key);
    }

    public void class1CountAddming(String key) {
        mapValueAdding(this.class1, key);
    }

    public void tag1CountAddming(String key) {
        mapValueAdding(this.tag1, key);
    }

    public void keywordCountAddming(String key) {
        mapValueAdding(this.keyword, key);
    }

    private static LinkedHashMap<String, Double> sortMapByValue(Map<String, Double> oriMap) {
        LinkedHashMap<String, Double> sortedListMap = new LinkedHashMap<>();
        if (oriMap != null && !oriMap.isEmpty()) {
            List<Map.Entry<String, Double>> entryList = new ArrayList<>(oriMap.entrySet());
            Collections.sort(entryList,
                    new Comparator<Map.Entry<String, Double>>() {
                        public int compare(Map.Entry<String, Double> entry1,
                                           Map.Entry<String, Double> entry2) {
                            return entry2.getValue().compareTo(entry1.getValue());
                        }
                    });
            Iterator<Map.Entry<String, Double>> iter = entryList.iterator();
            Map.Entry<String, Double> tmpEntry;
            while (iter.hasNext()) {
                tmpEntry = iter.next();
                sortedListMap.put(tmpEntry.getKey(), tmpEntry.getValue());
            }
        }
        return sortedListMap;
    }

    public void sorted() {
        this.imei = sortMapByValue(this.imei);
        this.location = sortMapByValue(this.location);
        this.class1 = sortMapByValue(this.class1);
        this.tag1 = sortMapByValue(this.tag1);
        this.keyword = sortMapByValue(this.keyword);
    }


    public String toJSON(){
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.writeValueAsString(this);
        } catch (IOException e) {
            return "";
        }
    }

    public static TagBean toTagBean(String json){
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.readValue(json, TagBean.class);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static void getMerge(TagBean tb1, TagBean tb2) {
        if(tb1.phone.equals(tb2.phone)) {
            mapValueMerging(tb1.imei, tb2.imei);
            mapValueMerging(tb1.location, tb2.location);
            mapValueMerging(tb1.class1, tb2.class1);
            mapValueMerging(tb1.tag1, tb2.tag1);
            mapValueMerging(tb1.keyword, tb2.keyword);
        }
    }

    public static void main(String[] args) {
        TagBean tb1 = new TagBean();
        tb1.setPhone("1370001111");
        tb1.imeiCountAddming("imei1");
        tb1.imeiCountAddming("imei1");

        System.out.println(tb1.toJSON());

        TagBean tb2 = new TagBean();
        tb2.setPhone("1370001111");
        tb2.imeiCountAddming("imei1");
        tb2.locationCountAddming("aaaaa");
        tb2.locationCountAddming("aaaaa");

        System.out.println(tb2.toJSON());

        getMerge(tb1, tb2);

        System.out.println(tb1.toJSON());

    }
}
