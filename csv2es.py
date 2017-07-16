#!/usr/bin/python
# -*- coding: utf-8 -*-
import click
import json
import itertools
from elasticsearch import helpers
from elasticsearch import Elasticsearch

# tranform a csv line into a list
def divide(line):
    new_line = ''.join(['[', line.strip('\n').replace('NULL', 'null'), ']'])
    try:
        return json.loads(new_line, strict=False)
    except ValueError as e:
        log('error', e.message)
        print new_line

# split an iterator into chunks of given size
def grouper(n, iterable):
    it = iter(iterable)
    while True:
       chunk = tuple(itertools.islice(it, n))
       if not chunk:
           return
       yield chunk

def bulk_builder(bulk, config):
    for item in bulk:
        body = {'_index': config['index'],
                '_type': config['type'],
                '_source': item}
        if config['id_field']:
            body['_id'] = item[config['id_field']]
            del item[config['id_field']]
        yield body


def index(bulk, config):
    bulk = bulk_builder(bulk, config)
    try:
        helpers.bulk(config['es_conn'], bulk)
    except Exception as e:
        log('warning', str(e))


def read (file, config):
    with open(file) as f:
        fields = divide(next(f))
        if config['offset']:
            for i in range(config['offset']):
                next(f)
        lines = (dict(zip(fields, values)) for values in (divide(line) for line in f) if values)
        # for line in lines:
        #   print line
        load(lines, config)

def load(lines, config):
    count = 0
    for bulk in grouper(config['bulk_size'], lines):
        index(bulk, config)
        count = count + 1
        # log('info', '%dx%d' % (count, config['bulk_size']))

def log(sevirity, msg):
    cmap = {'info': 'blue', 'warning': 'yellow', 'error': 'red'}
    click.secho(msg, fg=cmap[sevirity])


@click.command()
@click.argument('file', required=True)
@click.option('--es-host', default='http://localhost:9200', help='Elasticsearch cluster entry point. eg. http://localhost:9200')
@click.option('--bulk-size', default=500, help='How many docs to collect before writing to ElasticSearch')
# @click.option('--concurrency', default=10, help='How much worker threads to start')
@click.option('--index', help='Destination index name', required=True)
@click.option('--type', help='Docs type', required=True)
@click.option('--id-field', help='Specify field name that be used as document id')
@click.option('--offset', type=int, help='Pass the first offset lines')
@click.pass_context
def cli(ctx, file, **opts):
    ctx.obj = opts
    ctx.obj['es_conn'] = Elasticsearch(opts['es_host'])
    read(file, ctx.obj)

if __name__ == '__main__':
    cli()

