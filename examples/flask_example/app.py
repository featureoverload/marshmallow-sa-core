"""
requires:
  - marshmallow-sa-core
  - flask
  - flask-sqlalchemy
  - flask-smorest
"""

from flask import Flask
from flask.views import MethodView
from flask_smorest import Api, Blueprint, abort
from flask_sqlalchemy import SQLAlchemy
from marshmallow_sa_core import JSONTableSchema

app = Flask(__name__)
# docs
API_TITLE = 'Marshmallow SQLAlchemy Core Example API'
API_VERSION = '1.0.0'

app.config["API_TITLE"] = API_TITLE
app.config['API_VERSION'] = API_VERSION
app.config['OPENAPI_VERSION'] = '3.0.3'
app.config['OPENAPI_JSON_PATH'] = "api-spec.json"
app.config['OPENAPI_URL_PREFIX'] = "/"
app.config['OPENAPI_SWAGGER_UI_PATH'] = "/docs"
app.config['OPENAPI_SWAGGER_UI_URL'] = "https://cdn.jsdelivr.net/npm/swagger-ui-dist/"

api = Api(app)

# database
app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:////tmp/ma_sa_core_flask.db"
db = SQLAlchemy(app)

blp = Blueprint('app', __name__, url_prefix='')

from marshmallow import Schema
from marshmallow import fields

class CreateTableStatus(Schema):
    code = fields.Integer(required=True)
    message = fields.String(required=True)


@blp.route("/")
class Guide(MethodView):
    def get(self):
        return {'TODO': 'example guide'}


@blp.route("/tables/")
class TableList(MethodView):
    @blp.arguments(JSONTableSchema)
    @blp.response(201, CreateTableStatus)
    def post(self, table: 'Table'):
        """create table"""
        from sqlalchemy import inspect
        if inspect(db.engine).has_table(table.name):
            return abort(400, message=f'Table Name {table.name} Already Exists.')

        try:
            table.create(db.session.bind, checkfirst=True)
        except Exception as exc:
            return {'code': 1, 'message': str(exc)}
        else:
            return {'code': 0, 'message': 'success'}

    @blp.response(200, JSONTableSchema(many=True))
    def get(self):
        return abort(400, message='TODO: finish me')


@blp.route("/tables/<id>")
class Table(MethodView):
    @blp.response(200, JSONTableSchema)
    def get(self, id):
        return abort(400, message='TODO: finish me')

    @blp.response(204)
    def delete(self, id):
        return abort(400, message='TODO: finish me')

    def put(self, id, json_data):
        # TODO: marshmallow-sa-core should support ALTER
        return abort(400, message='TODO: finish me')


@app.before_first_request
def create_tables():
    db.create_all()


api.register_blueprint(blp)

if __name__ == "__main__":
    app.run(debug=True, port=5000)
