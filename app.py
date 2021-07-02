import flask
from flask import request, jsonify
#import sqlite3
from sqlite3worker2 import Sqlite3Worker


app = flask.Flask(__name__)
app.config["DEBUG"] = True
sql_worker = Sqlite3Worker("timings.db")

def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


@app.route('/', methods=['GET'])
def home():
    return '''<h1>Distant Reading Archive</h1>
<p>A prototype API for distant reading of science fiction novels.</p>'''


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
    if query_parameters and not (district_id or center_id or pincode):
        return page_not_found(404)

    query = query[:-4] + ';'

    # conn = sqlite3.connect('timings.db')
    # conn.row_factory = dict_factory
    # cur = conn.cursor()

    # results = cur.execute(query, to_filter).fetchall()
    results = sql_worker.execute(query, to_filter)

    return jsonify(results)

app.run()
sql_worker.close()