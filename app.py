import flask
import gzip
from flask import request, jsonify, make_response, json
from flask_cors import CORS
from sqlite3worker2 import Sqlite3Worker


app = flask.Flask(__name__)
app.config["DEBUG"] = True
CORS(app)
sql_worker = Sqlite3Worker("timings.db")

def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

def compress_response(payload, status_code=200):
    content  = gzip.compress(json.dumps(payload).encode('utf8'), 5)
    response = make_response(content, status_code)
    response.headers['Content-length'] = len(content)
    response.headers['Content-Encoding'] = 'gzip'
    return response
    
@app.route('/', methods=['GET'])
def home():
    return '''<h1>CoWIN Center Timings</h1>
<p>A prototype API for getting center timings.</p>'''


@app.route('/api/v1/centers/all', methods=['GET'])
def api_all():
    # conn = sqlite3.connect('timings.db')
    # conn.row_factory = dict_factory
    # cur = conn.cursor()
    all_centers = sql_worker.execute('SELECT * FROM centers')

    return jsonify(all_centers)

@app.errorhandler(404)
def page_not_found(e):
    return "<h1>404</h1><p>The resource could not be found.</p>", 404


@app.route('/api/v1/centers/timings', methods=['GET'])
def api_filter():
    query_parameters = request.args

    district_id = query_parameters.get('district_id')
    center_id   = query_parameters.get('center_id')
    pincode     = query_parameters.get('pincode')
    age         = query_parameters.get('age')
    past_days   = query_parameters.get('past_days')

    query = "SELECT c.center_id, c.pincode, c.name, t.district_id, t.posting_ts, t.slots, t.age, t.vaccine FROM centers c , timings t WHERE c.center_id = t.center_id AND"

    to_filter = []

    if district_id:
        query += ' t.district_id=? AND'
        to_filter.append(district_id)
    if center_id:
        query += ' t.center_id=? AND'
        to_filter.append(center_id)
    if pincode:
        query += ' c.pincode=? AND'
        to_filter.append(pincode)
    if age:
        query += ' t.age=? AND'
        to_filter.append(age)
    if past_days:
        temp = "(SELECT DATETIME('now', '-%s day'))"%past_days
        query += " t.posting_ts > (SELECT DATETIME('now', '-%s day')) AND"%past_days
        #to_filter.append(temp)
    if query_parameters and not (district_id or center_id or age or pincode or past_days):
        return page_not_found(404)

    query = query[:-4] + ';'

    # conn = sqlite3.connect('timings.db')
    # conn.row_factory = dict_factory
    # cur = conn.cursor()

    # results = cur.execute(query, to_filter).fetchall()
    results = sql_worker.execute(query, to_filter)

    #return make_response(jsonify(results), 200)
    return compress_response(results, 200)

if __name__ == '__main__':
    # Threaded option to enable multiple instances for multiple user access support
    app.run(threaded=True, port=5000)
    sql_worker.close()
