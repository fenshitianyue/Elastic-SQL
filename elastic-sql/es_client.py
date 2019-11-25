# -*- coding:UTF-8 -*-


class RestSQLQuery(object):
    q_from = ""
    q_filter = dict()
    q_group_by = []
    q_fields = []
    q_aggregation = []
    q_sort = []
    q_limit = 2000


class EsClient(object):
    def __init__(self, es_url):
        self.es_url = es_url
        # self.client = Elasticsearch(self.es_url.split(','))

    def query(self, query):
        dsl = {
            "size": query.q_limit,
            "query": {
                "bool": {
                    "must": [],
                    "filter": []
                }
            },
            "_source": {
                "includes": []
            },
            "aggregations": {
                "groupby": {
                    "composite": {
                        "size": query.q_limit,
                        "sources": []
                    },
                    "aggregations": {}
                }
            }
        }

        dsl["_source"]["includes"] = query.q_fields
        # dsl_where = dsl["query"]["bool"]["must"]  # TODO:where暂时都用query来查询，后期把能优化的优化为filter
        dsl_where = dsl["query"]["bool"]
        dsl_group_by = dsl["aggregations"]["groupby"]["composite"]["sources"]
        dsl_group_aggr = dsl["aggregations"]["groupby"]["aggregations"]

        # s = Search(using=self.client, index=q.q_from)
        for field, value in q.q_filter.items():
            if "__" not in field:  # 完全匹配
                dsl_where['filter'].append({
                    "term": {
                      field: value
                    }
                  })
            else:
                op = field.split("__")[1]
                field_name = field.split("__")[0]
                if op == "gt":
                    dsl_where['filter'].append({
                      "range": {
                        field_name: {"from": value, "to": None, "include_lower": False, "include_upper": False}
                      }
                    })
                elif op == "lt":
                    dsl_where['filter'].append({
                      "range": {
                        field_name: {"from": None, "to": value, "include_lower": False, "include_upper": False}
                      }
                    })
                elif op == "gte":
                    dsl_where['filter'].append({
                        "range": {
                            field_name: {"from": value, "to": None, "include_lower": True, "include_upper": False}
                        }
                    })
                elif op == "lte":
                    dsl_where['filter'].append({
                        "range": {
                            field_name: {"from": None, "to": value, "include_lower": False, "include_upper": True}
                        }
                    })
                elif op == "contains" or op == "startwith" or op == "endwith":
                    dsl_where['must'].append({
                        "match_phrase": {
                            field_name: {
                                "query": value
                            }
                        }
                    })
                elif op == "range":
                    dsl_where.append({
                        "range": {  # 这里预估value是一个只包含两个元素的列表
                            field_name: {"from": value[0], "to": value[1], "include_lower": True, "include_upper": True}
                        }
                    })
                elif op == "in":
                    dsl_where['filter'].append({
                        "terms": {
                            field_name: value  # 这里预估value是一个列表
                        }
                    })
                else:
                    raise ValueError("can NOT accept op: %s, field: %s" % (op, field))

        if query.q_group_by:
            for field in query.q_group_by:
                dsl_group_by.append({field: {"terms": {"field": field}}})
            for field in query.q_aggregation:
                field_name, func = field.split("__")[0], field.split("__")[1]
                func_map = {"count": "value_count", "sum": "sum", "avg": "avg", "max": "max", "min": "min"}
                if func in func_map:
                    dsl_group_aggr[field] = {func_map[func]: {"field": field_name}}
                else:
                    raise ValueError("can NOT accept aggr func: " + func)
        else:
            del dsl["aggregations"]

        print dsl
        # response = self.client.search(index=query.q_from, body=dsl)
        # return self.response_to_records(response)

    @staticmethod
    def response_to_records(response):
        records = []
        if "aggregations" in response:
            records = response["aggregations"]["groupby"]["buckets"]
            for r in records:
                r.update(r["key"])
                del r["key"]
                for k, v in r.items():
                    if isinstance(v, dict) and "value" in v:
                        r[k] = v["value"]
        elif "hits" in response and "hits" in response["hits"]:
            records = list(map(lambda x: x["_source"], response["hits"]["hits"]))
        print("response.records", records)
        return records


if __name__ == "__main__":
    # es = EsClient(settings.ATHENA_ES_URL)
    es = EsClient('url')
    q = RestSQLQuery()
    q.q_from = "metrics-daily-20191104"
    q.q_filter = {"category": "PO.MODEL_OS_STAT", "value__gt": 1000}
    q.q_group_by = ["time", "app_id"]
    q.q_aggregation = ["value__sum"]
    q.q_fields = ["app_id", "version", "id"]
    q.q_limit = 10
    es.query(q)


