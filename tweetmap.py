from flask import Flask, request, render_template, jsonify
from subprocess import *
import random

application = Flask(__name__)


@application.route("/", methods=['GET', 'POST'])
def deal_with_coordinate():
    if request.method == "POST":
        if request.form['check'] == "true":
            lt = request.form["lat"]
            lg = request.form["lng"]
            query = "curl -XGET https://search-twitter-test-2-zx2ikzsfpdsbfukjjlmcsxuexy.us-east-2.es.amazonaws.com/my_location/location/_search -d"+"'"+'{"query":{"bool":{"must":{"match_all":{}},"filter":{"geo_distance":{"distance":"200km","pin.location":{"lat":'+lt+',"lon":'+lg+'}}}}}}'+"'"
            t = Popen(query, shell=True, stdout=PIPE)
            text = t.communicate()[0]
            false = "false"
            new_json = eval(text)
            new_json['timed_out'] = "false"
            # idx = random.randint(0, len(new_json['hits']['hits'])-1)
            # return_dict = {"backendcoor":[{"lat": new_json['hits']['hits'][idx]['_source']['pin']['location']['lat'], "lng": new_json['hits']['hits'][idx]['_source']['pin']['location']['lon'], "text":new_json['hits']['hits'][idx]['_source']['text']}, ]}
            # if 'name' not in new_json:
            #     return_dict['name'] = "Null"
            # else:
            #     return_dict['name'] = new_json['name']
            return_dict = {"backendcoor": []}
            for idx in range(0, len(new_json['hits']['hits'])):
                return_dict["backendcoor"].append(
                    {"lat": new_json['hits']['hits'][idx]['_source']['pin']['location']['lat'],
                     "lng": new_json['hits']['hits'][idx]['_source']['pin']['location']['lon'],
                     "text": new_json['hits']['hits'][idx]['_source']['text']})
            return jsonify(return_dict)
        else:
            key_word = request.form['keyword']
            # print key_word
            query = 'curl -XGET https://search-twitter-test-2-zx2ikzsfpdsbfukjjlmcsxuexy.us-east-2.es.amazonaws.com/my_location/location/_search? -d '+"'"+'{"query":{"match":{"keyword":"'+key_word+'"}}}'+"'"
            # print query
            t = Popen(query, shell=True, stdout=PIPE)
            text = t.communicate()[0]
            false = "false"
            new_json = eval(text)
            # print new_json
            return_dict = {"backenddata": []}
            for idx in range(0, len(new_json['hits']['hits'])):
                return_dict["backenddata"].append({"lat": new_json['hits']['hits'][idx]['_source']['pin']['location']['lat'],
                               "lng": new_json['hits']['hits'][idx]['_source']['pin']['location']['lon'],
                               "text": new_json['hits']['hits'][idx]['_source']['text']})
            print  return_dict
            # for idx in range(len(new_json['hits']['hits'])):
            #     return_dict
            return jsonify(return_dict)

    return render_template("test.html")


if __name__ == "__main__":
    application.run()


