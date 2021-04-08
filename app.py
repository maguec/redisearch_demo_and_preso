from flask import Flask, render_template, request, redirect
from flask_bootstrap import Bootstrap
from flask_nav import Nav
from flask_nav.elements import Navbar, View
from redisearch import AutoCompleter, Suggestion, Client, Query, aggregation, reducers, IndexDefinition, TextField, NumericField, TagField

# From our local file
from dataload import load_data

from os import environ

import redis

import json
import string

app = Flask(__name__,
            static_url_path='/docs', 
            static_folder='docs',
)

bootstrap = Bootstrap()

if environ.get('REDIS_SERVER') is not None:
   redis_server = environ.get('REDIS_SERVER')
else:
   redis_server = 'localhost'

if environ.get('REDIS_PORT') is not None:
   redis_port = int(environ.get('REDIS_PORT'))
else:
   redis_port = 6379

if environ.get('REDIS_PASSWORD') is not None:
   redis_password = environ.get('REDIS_PASSWORD')
else:
   redis_password = ''

client = Client(
   'fortune500',
   host=redis_server,
   password=redis_password,
   port=redis_port
   )
ac = AutoCompleter(
   'ac',
   conn = client.redis
   )



nav = Nav()
topbar = Navbar('',
    View('Home', 'index'),
    View('Aggregations', 'show_agg'),
    View('CEO Search', 'search_ceo'),
    View('Tag Search', 'search_tags'),
    View('Presentation', 'preso'),
    View('Example Queries', 'example_queries'),
)
nav.register_element('top', topbar)

def agg_by(field):
   ar = aggregation.AggregateRequest().group_by(field, reducers.count().alias('my_count')).sort_by(aggregation.Desc('@my_count'))
   return (client.aggregate(ar).rows)

def search_data(company):
   print(Query(company).limit_fields('title').verbatim().summarize())
   j = client.search(Query(company).limit_fields('title').verbatim()).docs[0].__dict__
   del j['id']
   del j['payload']
   return(j)

@app.route('/')
def index():
   if ac.len() < 1:
       load_data(redis_server, redis_port, redis_password)
   return render_template('search.html')

@app.route('/display', methods = ['POST'])
def display():
   display = request.form
   info = search_data(display['account'])
   query = 'FT.SEARCH fortune500 "{}" INFIELDS 1 title VERBATIM LIMIT 0 1'.format(display['account'])
   return render_template('results.html', result = info, query=query )

@app.route('/aggregate')
def show_agg():
   return render_template("aggregate.html")

@app.route('/showagg', methods = ['POST'])
def agg_show():
   a = request.form.to_dict()
   rows = agg_by(a['agg'])
   # Filter and Capitalize the strings
   rows=[(lambda x: [string.capwords(x[1]), x[3]])(x) for x in rows]
   return render_template(
      'aggresults.html',
      rows = rows,
      query = 'FT.AGGREGATE fortune500 "*" GROUPBY 1 {} REDUCE COUNT 0 AS my_count SORTBY 2 @my_count DESC'.format(a['agg']),
      field = a['agg'].replace("@", '').capitalize())

@app.route('/autocomplete')
def auto_complete():
    name = request.args.get('term')
    suggest = ac.get_suggestions(name, fuzzy = True)
    return(json.dumps([{'value': item.string, 'label': item.string, 'id': item.string, 'score': item.score} for item in suggest]))

@app.route('/searchceo')
def search_ceo():
   return render_template("searchceo.html")

@app.route('/displayceo', methods=['POST'])
def display_ceo():
   form = request.form.to_dict()
   try:
      ceos = [(lambda x: [x.company, x.ceo, x.ceoTitle]) (x) for x in client.search(Query(form["ceo"]).limit_fields('ceo')).docs]
      return render_template(
         'displayceos.html',
         ceos = ceos,
         query='FT.SEARCH fortune500 "{}" INFIELDS 1 ceo LIMIT 0 10'.format(form["ceo"]),
      )
   except Exception as e:
      return "<html><body><script> var timer = setTimeout(function() { window.location='/searchceo' }, 5000); </script> Bad Query : %s try again with  &percnt;NAME&percnt;</body> </html>" % e

@app.route('/searchtags')
def search_tags():
   tags = client.tagvals("tags")
   return render_template("searchtags.html", tags=tags)

@app.route('/displaytags', methods=['POST'])
def display_tags():
   tags = request.form.getlist('tgs')
   q = Query("@tags:{%s}" %("|".join(tags))).sort_by('rank', asc=True).paging(0, 100)
   res = [(lambda x: [x.rank, x.company, x.tags]) (x) for x in client.search(q).docs]
   return render_template(
      'displaytags.html',
      query='FT.SEARCH fortune500 "@tags:{{{}}}" SORTBY rank ASC LIMIT 0 100'.format("|".join(tags)),
      companies = res)


@app.route('/preso')
def preso():
   return redirect("/docs/index.html", code=302)


@app.route('/example')
def example_queries():
   return render_template('example_queries.html')
if __name__ == '__main__':
   bootstrap.init_app(app)
   nav.init_app(app)
   app.debug = True
   app.run(port=5000, host="0.0.0.0")
