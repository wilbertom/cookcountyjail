#
# app.py is a Controller file and should do very little,
#        processing should be pushed down into model files.
#

from flask import Flask, jsonify, request, Response
from flask.json import dumps
from flask.ext.sqlalchemy import SQLAlchemy
from os import getcwd, path
from os.path import isfile, join
from datetime import datetime

from ccj.models.daily_population import DailyPopulation as DPC
from ccj import config

STARTUP_TIME = datetime.now()

app = Flask(__name__)
app.config.from_object(config)

db = SQLAlchemy(app)

if app.config['IN_TESTING']:
    app.debug = True

CURRENT_FILE_PATH = 'build_info/current'
PREVIOUS_FILE_PATH = 'build_info/previous'
VERSION_NUMBER = "2.0-dev"


@app.route('/daily_population', methods=['GET'])
def daily_population():
    """
    returns the set of summarized daily population changes.
    """
    return Response(DPC(app.config['DPC_DIR_PATH']).to_json(),  mimetype='application/json')


@app.route('/os_env_info')
def env_info():
    """
    Displays information about the current OS environment.
    Used for development purposes, to be deleted when this is no longer a dev branch.
    """
    r_val = jsonify(
        cwd=getcwd(),
        remote_addr=request.environ.get('REMOTE_ADDR', 'not set'),
        headers=str(request.headers),
        environ=str(request.environ)
        )
    return r_val


@app.route('/starting_population', methods=['GET'])
def starting_population():
    """
    returns the set of starting daily population values used to calculate daily changes.
    """
    return Response(dumps(DPC(app.config['DPC_DIR_PATH']).starting_population()),  mimetype='application/json')


@app.route('/version')
def version_info():
    """
    returns the version info
    """
    args = request.args
    if 'all' in args and args['all'] == '1':
        r_val = []
        previous_build_info('.', r_val)
    else:
        r_val = build_info(CURRENT_FILE_PATH)
    return Response(dumps(r_val),  mimetype='application/json')


def build_info(fname):
    return {'Version': VERSION_NUMBER, 'Build': current_build_info(fname), 'Deployed': deployed_at(fname)}


def current_build_info(fname):
    return file_contents(fname, 'running-on-dev-box')


def deployed_at(fname):
    if isfile(fname):
        mtime = path.getmtime(fname)
        r_val = datetime.fromtimestamp(mtime)
    else:
        r_val = STARTUP_TIME
    return str(r_val)


def file_contents(fname, default_rvalue):
    if isfile(fname):
        with open(fname, 'r') as f:
            return f.read().strip()
    return default_rvalue


def previous_build_info(dir_path, r_val):
    r_val.append(build_info(join(dir_path, CURRENT_FILE_PATH)))
    previous_fname = join(dir_path, PREVIOUS_FILE_PATH)
    if isfile(previous_fname):
        previous_build_info(join('..', file_contents(previous_fname, '')), r_val)
