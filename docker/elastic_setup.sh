#Create an index called “nyc-idx”:
curl -XPUT "http://es01:9200/nyc-idx"

#Create an index mapping called “popular-locations”:  
curl -XPUT "http://es01:9200/nyc-idx/_mapping/popular-locations" -d'
{
 "popular-locations" : {
   "properties" : {
      "cnt": {"type": "integer"},
      "location": {"type": "geo_point"},
      "time": {"type": "date"}
    }
 } 
}'