import imp
from celery import Celery
from app.config.constants import CeleryConstants as CC
from app.util.model_utils import deserialize_from_base64
from app.core.declensions import get_declensions
from uuid import UUID
import pymongo
import re
from ibex_models import CollectTask, SearchTerm, Post, Account, Monitor, CollectAction
from ibex_models.platform import Platform
from model import RequestPostsFilters, RequestPostsFiltersAggregated, RequestMonitor
import os
from turtle import st
from beanie import init_beanie
import motor
from beanie.odm.operators.find.comparison import In

from bson import json_util, ObjectId
from bson.json_util import dumps, loads
from pydantic import Field, BaseModel, validator

import json 
from fastapi.responses import JSONResponse, RedirectResponse
from fastapi.encoders import jsonable_encoder
from typing import List
import langid
import subprocess

def terminate_monitor_tasks(monitor_id: UUID):
    my_app = Celery("ibex tasks",
                    broker=CC.LOCAL_BROKER_URL_REDIS,
                    backend=CC.LOCAL_RESULT_BACKEND_REDIS,
                    include=[
                        'app.core.celery.tasks.collect', 
                        'app.core.celery.tasks.download', 
                        'app.core.celery.tasks.process']
                    )


    i = my_app.control.inspect()
    scheduled = i.scheduled()
    reserved = i.reserved()
    active = i.active()

    active_tasks = list(active.items())[0][1] if active else []
    reserved_tasks = list(reserved.items())[0][1] if reserved else []
    scheduled_tasks = list(scheduled.items())[0][1] if scheduled else []

    all_tasks = active_tasks + reserved_tasks + scheduled_tasks

    ids_to_kill = []
    for task in all_tasks:
        first_sub_task = deserialize_from_base64(task['kwargs']['it'][0])
        if first_sub_task.monitor_id == monitor_id:
            ids_to_kill.append(task['id'])

    my_app.control.revoke(ids_to_kill, terminate=True, signal='SIGUSR1')
    # len(active_tasks), len(reserved_tasks), len(scheduled_tasks), len(ids_to_kill)
    print(f'{len(ids_to_kill)} tasks terminated')
    

async def get_keywords_in_monitor(monitor_id):
    collect_tasks = await CollectTask.find(CollectTask.monitor_id == UUID(monitor_id)).to_list()
    getTerm = lambda collect_task: None if not collect_task.search_terms else collect_task.search_terms[0].term
    unique = set([getTerm(collect_task) for collect_task in collect_tasks])

    keywords_already_in_monitor = []
    for search_term in unique:
        if not search_term: continue
        keywords_already_in_monitor += re.split(' AND | OR | NOT ', search_term)

    return list(set(keywords_already_in_monitor))


async def generate_search_criteria(post_request_params: RequestPostsFilters):
    search_criteria = {}
    if post_request_params.time_interval_from or post_request_params.time_interval_to:
        search_criteria['created_at'] = {}
    if post_request_params.time_interval_from:
        search_criteria['created_at']['$gte'] = post_request_params.time_interval_from
    if post_request_params.time_interval_to:
        search_criteria['created_at']['$lte'] = post_request_params.time_interval_to

    if bool(post_request_params.search_term_ids) and len(post_request_params.search_term_ids) > 0:
        search_criteria['search_term_ids'] = { '$in': [UUID(id) for id in post_request_params.search_term_ids] }

    if bool(post_request_params.monitor_id):
        search_criteria['monitor_ids'] = { '$in': [UUID(post_request_params.monitor_id)] }  

    if type(post_request_params.has_video) == bool:
        search_criteria['has_video'] = { '$eq': post_request_params.has_video }

    if bool(post_request_params.post_contains):
        search_criteria['text'] = { '$regex': post_request_params.post_contains }

    if len(post_request_params.platform) > 0:
        search_criteria['platform'] = { '$in': post_request_params.platform }

    if post_request_params.account_ids and len(post_request_params.account_ids) > 0:
        search_criteria['account_id'] = { '$in': [UUID(id) for id in post_request_params.account_ids] }

    if len(post_request_params.author_platform_id) > 0:
        search_criteria['author_platform_id'] = { '$in': post_request_params.author_platform_id }
    
    if len(post_request_params.topics) > 0:
        search_criteria['labels.topics'] = { '$in': [UUID(i) for i in post_request_params.topics] }
    
    if len(post_request_params.persons) > 0:
        search_criteria['labels.persons'] = { '$in': [UUID(i) for i in post_request_params.persons] }

    if len(post_request_params.locations) > 0:
        search_criteria['labels.locations'] = { '$in': [UUID(i) for i in post_request_params.locations] }

    return search_criteria

def get_mongo_cs(request):
    sub_domain = request.url._url.split('.ibex-app.com')[0].split('//')[1]
    sub_domain = sub_domain if sub_domain in ['dev', 'un', 'isfed'] else 'dev'
    mongodb_connection_string = os.getenv(f'MONGO_CS')
    # if not mongodb_connection_string:
    #     raise 
    return mongodb_connection_string

async def mongo(classes, request):
    mongodb_connection_string = get_mongo_cs(request)

    client = motor.motor_asyncio.AsyncIOMotorClient(mongodb_connection_string)
    await init_beanie(database=client.ibex, document_models=classes)


def json_responce(result):
    json_result = json.loads(json_util.dumps(result))
    return JSONResponse(content=jsonable_encoder(json_result), status_code=200)

import pymongo

async def delete_out_of_monitor_posts(monitor_id:UUID, request):
    mongo_connection_string = get_mongo_cs(request)
    client = pymongo.MongoClient(mongo_connection_string)
    db = client["ibex"]

    posts           = db["posts"]
    search_terms    = db["search_terms"]
    accounts        = db["accounts"]

    only_this_monitor_query = {
        'monitor_ids': [monitor_id]
    }
    other_monitors_query = {
        'monitor_ids': {'$in': [monitor_id]}
    }

    search_term_ids = [_['_id'] for _ in search_terms.find({'tags' : {'$in': [str(monitor_id)]}})]
    account_ids = [_['_id'] for _ in accounts.find({'tags' : {'$in': [str(monitor_id)]}})]
    
    if len(search_term_ids):
        only_this_monitor_query['search_term_ids'] = {'$nin': search_term_ids}
        other_monitors_query['search_term_ids'] = {'$nin': search_term_ids}
    if len(account_ids):
        only_this_monitor_query['account_id'] = {'$nin': account_ids}
        other_monitors_query['account_id'] = {'$nin': account_ids}

    print('other_monitors_query', other_monitors_query)    
    print('query to delete', only_this_monitor_query)

    posts.delete_many(only_this_monitor_query)
    
    posts_shared_with_other_monitors = list(posts.find(other_monitors_query))

    if len(posts_shared_with_other_monitors):
        for post in posts_shared_with_other_monitors:
            post_ = await Post.get(post["_id"])
            post_.monitor_ids = [monnitor_id for monnitor_id in post_.monitor_ids if monnitor_id != monitor_id]
            await post_.save()


async def get_posts(post_request_params: RequestPostsFilters): 
    search_criteria = await generate_search_criteria(post_request_params)
    posts = await Post.find(search_criteria)\
        .aggregate([
            {   '$sort': { 'created_at': -1 }},
            {
                '$skip': post_request_params.start_index
            },
            {
                '$limit': post_request_params.count
            },
            {
                '$lookup': {
                        'from': "tags",
                        'localField': "labels.topics",
                        'foreignField': "_id",
                        'as': "labels.topics"
                    }
            },
            {
                '$lookup': {
                        'from': "tags",
                        'localField': "labels.locations",
                        'foreignField': "_id",
                        'as': "labels.locations"
                    }
            },
            {
                '$lookup': {
                        'from': "tags",
                        'localField': "labels.persons",
                        'foreignField': "_id",
                        'as': "labels.persons"
                    }
            },
            # {
            #     '$lookup': {
            #         'from': 'search_terms', 
            #         'localField': 'search_term_ids', 
            #         'foreignField': '_id', 
            #         'as': 'search_terms'}
            # }, 
            {
                '$lookup': {
                    'from': "accounts",
                    'localField': f"account_id",
                    'foreignField': "_id",
                    'as': "account"
                }
            },
        ])\
        .to_list()
    # print(22, search_criteria)
    # print(33, post_request_params)
    
    for post in posts:
        post['api_dump'] = ''

    return posts


async def modify_monitor_search_terms(postMonitor: RequestMonitor):
    # if search terms are passed, it needs to be compared to existing list and
    # and if changes are made, existing records needs to be modified
    # finding search terms in db which are no longer preseng in the post request
    print(postMonitor)
    postMonitor_search_terms = [_.term for _ in postMonitor.search_terms]
    db_search_terms: List[SearchTerm] = await SearchTerm.find(In(SearchTerm.tags, [str(postMonitor.id)])).to_list()
    db_search_terms_to_to_remove_from_db: List[SearchTerm] = [__ for __ in db_search_terms if __.term not in postMonitor_search_terms]
    # await SearchTerm.find()
    # client = pymongo.MongoClient("mongodb+srv://root:Dn9B6czCKU6qFCj@cluster0.iejvr.mongodb.net/ibex?retryWrites=true&w=majority")
    # mydb = client["ibex"]
    # col_posts = mydb["search_terms"]
    # query = {'tags':{'$nin': [str(postMonitor.id)]}}
    # col_posts.delete_many(query)
    # print('passed_search_terms:', [_.term for _ in postMonitor.search_terms])
    # print('db_search_terms:', [_.term for _ in db_search_terms])
    # print('db_search_terms_to_to_remove_from_db:', [_.term for _ in db_search_terms_to_to_remove_from_db])

    for search_term in db_search_terms_to_to_remove_from_db:
        search_term.tags = [tag for tag in search_term.tags if tag != str(postMonitor.id)]
        await search_term.save()

    # finding search terms that are not taged in db
    db_search_terms_strs: List[str] = [search_term.term for search_term in db_search_terms]
    search_terms_to_add_to_db: List[str] = [search_term for search_term in postMonitor_search_terms if
                                            search_term not in db_search_terms_strs]
    print('search_terms_to_add_to_db:', search_terms_to_add_to_db)
    
    searchs_to_insert = []
    for search_term_str in search_terms_to_add_to_db:
        db_search_term = await SearchTerm.find(SearchTerm.term == search_term_str).to_list()
        if len(db_search_term) > 0:
            # If same keyword exists in db, monitor.id is added to it's tags list
            db_search_term[0].tags.append(str(postMonitor.id))
            await db_search_term[0].save()
        else:
            # If keyword does not exists in db, new keyword is created
            searchs_to_insert.append(SearchTerm(term=search_term_str, tags=[str(postMonitor.id)]))

    if len(searchs_to_insert): await SearchTerm.insert_many(searchs_to_insert)


async def modify_monitor_accounts(postMonitor):
    
    # if accounts are passed, it needs to be compared to existing list and
    # and if changes are made, existing records needs to be modified
    # accounts: List[RequestAccount]
    db_accounts: List[SearchTerm] = await Account.find(In(Account.tags, [str(postMonitor.id)])).to_list()
    db_accounts_terms_to_remove_from_db: List[Account] = [_ for _ in db_accounts if _.id not in [_.id for _ in postMonitor.accounts]]
    print('len db_accounts' , len(db_accounts))
    print('len db_accounts_terms_to_remove_from_db' , len(db_accounts_terms_to_remove_from_db))
    for account in db_accounts_terms_to_remove_from_db:
        account.tags = [tag for tag in account.tags if tag != str(postMonitor.id)]
        await account.save()
    
    accounts_to_insert = [Account(
        title=account.title,
        platform=account.platform,
        platform_id=account.platform_id,
        tags=[str(postMonitor.id)],
        url='') for account in postMonitor.accounts if not account.id]
    print('len accounts_to_insert' , len(accounts_to_insert))

    if len(accounts_to_insert): await Account.insert_many(accounts_to_insert)


def collect_data_cmd(monitor_id:str, sample:bool = False):
    cmd = f'python3 /root/data-collection-and-processing/main.py --monitor_id={monitor_id} {"--sample" if sample else ""} >> celery.out'
    print(f'running command for data collection: {cmd}')
    subprocess.Popen(cmd, stdout=None, stderr=None, stdin=None, close_fds=True, shell=True)


async def get_keywords_in_monitor(monitor_id, use_declensions = False):
    collect_tasks = await CollectTask.find(CollectTask.monitor_id == UUID(monitor_id)).to_list()
    getTerm = lambda collect_task: None if not collect_task.search_terms else collect_task.search_terms[0].term
    unique = set([getTerm(collect_task) for collect_task in collect_tasks])

    keywords_already_in_monitor = []
    for search_term in unique:
        if not search_term: continue
        keywords_already_in_monitor += re.split(' AND | OR | NOT ', search_term)
    
    unique_keywords_already_in_monitor = list(set(keywords_already_in_monitor))
    if not use_declensions:
        return unique_keywords_already_in_monitor
    
    unique_keywords_already_in_monitor_with_decl = []
    for keyword in unique_keywords_already_in_monitor:
        unique_keywords_already_in_monitor_with_decl += get_declensions([keyword], langid.classify(keyword)[0]) 

    return unique_keywords_already_in_monitor_with_decl


async def get_posts_aggregated(post_request_params_aggregated: RequestPostsFiltersAggregated):
    search_criteria = await generate_search_criteria(post_request_params_aggregated.post_request_params)
    
    axisX = f"${post_request_params_aggregated.axisX}" \
        if post_request_params_aggregated.axisX in ['platform', 'author_platform_id', 'search_term_ids', 'account_id'] \
        else f"$labels.{post_request_params_aggregated.axisX}" 

    aggregation = {}
    if post_request_params_aggregated.days is None:
        aggregation = {
            "label": axisX,
        }
    else:
        aggregation = {
            "label": axisX,
            "year": { "$year": "$created_at" },
        }
        if post_request_params_aggregated.days == 30:
            aggregation["month"] = { "$month": "$created_at" }
        if post_request_params_aggregated.days == 7:
            aggregation["week"] = { "$week": "$created_at" }
        if post_request_params_aggregated.days == 1:
            aggregation["day"] = { "$dayOfYear": "$created_at" }
    
    aggregations = []    
    if post_request_params_aggregated.axisX not in ['platform', 'author_platform_id']:
        aggregations.append({'$unwind':axisX })

    group = {'$group': {
        '_id': aggregation , 
        'count': {'$sum':None}
        } 
    }

    if post_request_params_aggregated.axisY == 'count':
        group['$group']['count']['$sum'] = 1
    if post_request_params_aggregated.axisY in ['like', 'comment', 'total']:
        group['$group']['count']['$sum'] = '$scores.' + post_request_params_aggregated.axisY

    aggregations.append(group)
    
    if post_request_params_aggregated.axisX not in ['platform', 'author_platform_id', 'search_term_ids', 'account_id']:
        aggregations.append({
                '$lookup': {
                    'from': "tags",
                    'localField': f"_id{'.label' if post_request_params_aggregated.days is not None else ''}",
                    'foreignField': "_id",
                    'as': f"{post_request_params_aggregated.axisX}"
                }
            })
        aggregations.append({'$unwind': f"${post_request_params_aggregated.axisX}" })
    
    if post_request_params_aggregated.axisX == 'search_term_ids':
        aggregations.append({'$lookup': {'from': 'search_terms', 'localField': '_id.label', 'foreignField': '_id', 'as': 'search_term_ids'}})
        aggregations.append({'$unwind': f"$search_term_ids" })
        aggregations.append({'$set': { 'term': '$search_term_ids.term' }})
    elif post_request_params_aggregated.axisX == 'account_id':
        aggregations.append({'$lookup': {'from': 'accounts', 'localField': '_id.label', 'foreignField': '_id', 'as': 'account_id'}})
        aggregations.append({'$unwind': '$account_id'})
        aggregations.append({'$set': { 'account_title': '$account_id.title', 'platform': '$account_id.platform'}})
    elif post_request_params_aggregated.axisX == 'platform':
        aggregations.append({'$set': {  'platform': '$platform.label', 'label': '$_id.label' }})
        aggregations.append({'$project': { 'platform':0}})

    else:
        set_ = { '$set': {} }
        set_['$set'][post_request_params_aggregated.axisX] = '$_id'
        aggregations.append(set_)
    aggregations.append({'$set':{
        'year': '$_id.year',
        'week': '$_id.week',
        'day': '$_id.day',
    }})
    aggregations.append({'$project': { 'account_id':0, '_id':0 , 'search_term_ids':0}})
    # print('aggr', search_criteria)
    # print('aggr', aggregations)
    result = await Post.find(search_criteria)\
        .aggregate([
            *aggregations,
        ])\
        .to_list()

    return result

async def update_collect_actions(monitor: Monitor):
    collect_actions_in_db: List[CollectAction] = await CollectAction.find(CollectAction.monitor_id == monitor.id).to_list()
    
    platforms = [platform for platform in monitor.platforms if len([_ for _ in collect_actions_in_db if _.platform == platform]) == 0]
    if len(platforms):
        collect_actions = [CollectAction(
                monitor_id = monitor.id,
                platform = platform, 
                search_term_tags = [str(monitor.id)], 
                account_tags=[str(monitor.id)],
                tags = [],
            ) for platform in platforms]
        print(f'Inserting {len(collect_actions)} collect actions...')
        await CollectAction.insert_many(collect_actions)
    
async def fetch_full_monitor(monitor_id: str):    
    monitor = await Monitor.get(monitor_id)
    search_terms = await SearchTerm.find(In(SearchTerm.tags, [monitor_id])).to_list()
    accounts = await Account.find(In(SearchTerm.tags, [monitor_id])).to_list()
    # collect_actions = await CollectAction.find(CollectAction.monitor_id == UUID(monitor_id)).to_list()
    # platforms = set([collect_action.platform for collect_action in collect_actions])
    monitor_dict = {
        '_id': monitor.id,
        'title': monitor.title,
        'descr': monitor.descr,
        'platforms': monitor.platforms,
        'date_to': monitor.date_to,
        'date_from': monitor.date_from,
        'status': monitor.status,
        'search_terms': [_.__dict__ for _ in search_terms],
        'accounts': [_.__dict__ for _ in accounts],
    }
    return { 'db_monitor': monitor, 'full_monitor': monitor_dict }