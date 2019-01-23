#!/usr/bin/env python

import click
import logging
from pyspider.database.base.projectdb import ProjectDB
from pyspider.database.base.taskdb import TaskDB
from pyspider.database.base.resultdb import ResultDB
from pyspider.database import connect_database
from pyspider.libs.utils import unicode_obj
from multiprocessing.pool import ThreadPool as Pool

logging.getLogger().setLevel(logging.INFO)

@click.command()
@click.option('--pool', default=10, help='cocurrent worker size.')
@click.argument('from_connection', required=1)
@click.argument('to_connection', required=1)
def migrate(pool, from_connection, to_connection):
    """
    Migrate tool for pyspider
    """
    f = connect_database(from_connection)
    t = connect_database(to_connection)

    if isinstance(f, ProjectDB):
        for each in f.get_all():
            each = unicode_obj(each)
            logging.info("projectdb: %s", each['name'])
            t.drop(each['name'])
            t.insert(each['name'], each)
    elif isinstance(f, TaskDB):
        pool = Pool(pool)
        pool.map(
            lambda x, f=from_connection, t=to_connection: taskdb_migrating(x, f, t),
            f.projects)
    elif isinstance(f, ResultDB):
        pool = Pool(pool)
        pool.map(
            lambda x, f=from_connection, t=to_connection: resultdb_migrating(x, f, t),
            f.projects)


if __name__ == '__main__':
    migrate()