# Kafka-Input-Handler
Выпускной проект по курсу otus DE 2020-02
## Описание 
Требования: https://docs.google.com/document/d/1RRBApzUUkYUsvW1TtL5g2XMVimWcVSf5TKkVH2jHHLM/edit?usp=sharing <br />
Презентация: https://docs.google.com/presentation/d/1cNDN-SFYyOhr8QRvxGpnShyadTsw_mn1GQmXFmrKaQ8/edit?usp=sharing

Формат принимаемого сообщения на загрузку данных: 
  | Имя поля | Тип | Описание | 
  | ------ | ------ | ------ |
  | version | строка | Идентификатор версии порции данных
  | kafkaSource | объект | Объект с данными по кафке
  | kafkaSource.topic | строка | целевой топик откуда получить данные
  | kafkaSource.startingOffsets | объект | см.документацию по спарку о формате https://spark.apache.org/docs/2.1.2/structured-streaming-kafka-integration.html#creating-a-kafka-source-batch
  | kafkaSource.endingOffsets | объект | см.документацию по спарку о формате https://spark.apache.org/docs/2.1.2/structured-streaming-kafka-integration.html#creating-a-kafka-source-batch
  | schema | строка | Схема авро
  | totalRecords | число | Количество записей после дешифровки авро формата
  | totalMessages | число | Количество записей в кафке в которых передана порция данных
  | params | объект | вспомогательный объект с дополнительным параметрами
  | params.path | строка | путь по которому записать порцию данных 

Пример:
```json
{
  "version": "Protocol_1555948100124_207_2020-07-01",
  "kafkaSource": {
    "topic": "load.paneldemvalue",
    "startingOffsets": {
      "load.paneldemvalue": {
        "0": 0
      }
    },
    "endingOffsets": {
      "load.paneldemvalue": {
        "0": 1
      }
    }
  },
  "schema": "{\"type\" : \"record\",\"name\" : \"Protocol\",\"namespace\" : \"load.monitoring\",\"fields\" : [ {  \"name\" : \"action\",  \"type\" : [ \"string\" ]}, {  \"name\" : \"VersionID\",  \"type\" : [ \"int\" ]}, {  \"name\" : \"TaskID\",  \"type\" : [ \"int\" ]}, {  \"name\" : \"BatchID\",  \"type\" : [ \"int\" ]}, {  \"name\" : \"Reload\",  \"type\" : [ \"boolean\" ]}, {  \"name\" : \"SysTime\",  \"type\" : [ {    \"type\" : \"long\",    \"logicalType\" : \"timestamp-millis\"  } ]}, {  \"name\" : \"LoadID\",  \"type\" : [ \"int\" ]}, {  \"name\" : \"MapDate\",  \"type\" : [ {    \"type\" : \"int\",    \"logicalType\" : \"date\"  } ]}, {  \"name\" : \"ChannelID\",  \"type\" : [ \"int\" ]}, {  \"name\" : \"CompanyID\",  \"type\" : [ \"int\" ]}, {  \"name\" : \"StartTime\",  \"type\" : [ {    \"type\" : \"long\",    \"logicalType\" : \"timestamp-millis\"  } ]}, {  \"name\" : \"Duration\",  \"type\" : [ \"int\" ]}, {  \"name\" : \"LocalCompanyID\",  \"type\" : [ \"int\" ]}, {  \"name\" : \"LocalTime\",  \"type\" : [ {    \"type\" : \"long\",    \"logicalType\" : \"timestamp-millis\"  } ]}, {  \"name\" : \"OriginalChannelID\",  \"type\" : [ \"int\" ]}, {  \"name\" : \"OriginalCompanyID\",  \"type\" : [ \"int\" ]}, {  \"name\" : \"OriginalTime\",  \"type\" : [ {    \"type\" : \"long\",    \"logicalType\" : \"timestamp-millis\"  } ]} ],\"version\" : \"Protocol_1555948100124_207_2020-07-01\",\"BatchID\" : 207,\"DataDate\" : \"2020-07-01\"}",
  "totalRecords": 1,
  "totalMessages": 1,
  "params": {
    "path": "/tmp/version=123"
  }
}
```

Формат сообщения на отправку результата выполнения:
  | Имя поля | Тип | Описание | 
  | ------ | ------ | ------ |
  | task | объект | объект с атрибутами выполняемой задачи
  | status | enum | статус выполнения, допустимые значения - OK|ERROR
  | description | строка | Если в поле status значение ERROR, то заполняется это поле и пишется ошибка

Пример:
```json
{
   "task": {
      "version":"Protocol_1555948100124_207_2020-07-01",
      "kafkaSource":{
         "topic":"load.paneldemvalue",
         "startingOffsets":{
            "load.paneldemvalue":{
               "0":0
            }
         },
         "endingOffsets":{
            "load.paneldemvalue":{
               "0":1
            }
         }
      },
      "schema":"{\"type\" : \"record\",\"name\" : \"Protocol\",\"namespace\" : \"load.monitoring\",\"fields\" : [ {  \"name\" : \"action\",  \"type\" : [ \"string\" ]}, {  \"name\" : \"VersionID\",  \"type\" : [ \"int\" ]}, {  \"name\" : \"TaskID\",  \"type\" : [ \"int\" ]}, {  \"name\" : \"BatchID\",  \"type\" : [ \"int\" ]}, {  \"name\" : \"Reload\",  \"type\" : [ \"boolean\" ]}, {  \"name\" : \"SysTime\",  \"type\" : [ {    \"type\" : \"long\",    \"logicalType\" : \"timestamp-millis\"  } ]}, {  \"name\" : \"LoadID\",  \"type\" : [ \"int\" ]}, {  \"name\" : \"MapDate\",  \"type\" : [ {    \"type\" : \"int\",    \"logicalType\" : \"date\"  } ]}, {  \"name\" : \"ChannelID\",  \"type\" : [ \"int\" ]}, {  \"name\" : \"CompanyID\",  \"type\" : [ \"int\" ]}, {  \"name\" : \"StartTime\",  \"type\" : [ {    \"type\" : \"long\",    \"logicalType\" : \"timestamp-millis\"  } ]}, {  \"name\" : \"Duration\",  \"type\" : [ \"int\" ]}, {  \"name\" : \"LocalCompanyID\",  \"type\" : [ \"int\" ]}, {  \"name\" : \"LocalTime\",  \"type\" : [ {    \"type\" : \"long\",    \"logicalType\" : \"timestamp-millis\"  } ]}, {  \"name\" : \"OriginalChannelID\",  \"type\" : [ \"int\" ]}, {  \"name\" : \"OriginalCompanyID\",  \"type\" : [ \"int\" ]}, {  \"name\" : \"OriginalTime\",  \"type\" : [ {    \"type\" : \"long\",    \"logicalType\" : \"timestamp-millis\"  } ]} ],\"version\" : \"Protocol_1555948100124_207_2020-07-01\",\"BatchID\" : 207,\"DataDate\" : \"2020-07-01\"}",
      "totalRecords":1,
      "totalMessages":1,
      "params":{
         "path":"/tmp/version=123"
      }
   },
   "stasus":"OK",
   "description":""
}
```
