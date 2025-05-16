import pandas as pd
from flask import Blueprints
from flask import render_template, redirect, url_for
from DatabaseTools.MongoDBTools.MongoDBTools import MongoDB


main = Blueprints('main', __name__)


def get_strategies()->list:
    mongo = MongoDB('BackTestResults')
    tested_strategies = mongo.show_collections()
    return tested_strategies


def get_tested_results(strategy:str, test_date:str)->pd.DataFrame:
    mongo = MongoDB('BackTestResults')
    results = mongo.get_test_result(strategy, test_date)
    return results


@main.route('/')
def index():
    return redirect(url_for('main.detail'))


@main.route('/', method=['GET'])
def detail():
    pass


