import imp
from celery import Celery
from app.config.constants import CeleryConstants as CC
from app.util.model_utils import deserialize_from_base64
from uuid import UUID
import re
from ibex_models import CollectTask, SearchTerm, Post, Account, Monitor, CollectAction
from ibex_models.platform import Platform
from model import RequestPostsFilters, RequestPostsFiltersAggregated
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

    active_tasks = list(active.items())[0][1]
    reserved_tasks = list(reserved.items())[0][1]
    scheduled_tasks = list(scheduled.items())[0][1]

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
    search_criteria = {
        # 'created_at': { '$gte': post_request_params.time_interval_from, '$lte': post_request_params.time_interval_to},
    }

    if bool(post_request_params.search_terms) and len(post_request_params.search_terms) > 0:
        search_terms = await SearchTerm.find({'term': {'$in': post_request_params.search_terms}}).to_list()
        search_criteria['search_terms_ids'] = { '$in': [search_term.id for search_term in search_terms] }

    if bool(post_request_params.monitor_id):
        search_criteria['monitor_ids'] = { '$in': [UUID(post_request_params.monitor_id)] }  

    if type(post_request_params.has_video) == bool:
        search_criteria['has_video'] = { '$eq': post_request_params.has_video }

    if bool(post_request_params.post_contains):
        search_criteria['text'] = { '$regex': post_request_params.post_contains }

    if len(post_request_params.platform) > 0:
        search_criteria['platform'] = { '$in': post_request_params.platform }

    if len(post_request_params.accounts) > 0:
        search_criteria['account_id'] = { '$in': [Binary(i.bytes, 3) for i in post_request_params.accounts] }

    if len(post_request_params.author_platform_id) > 0:
        search_criteria['author_platform_id'] = { '$in': post_request_params.author_platform_id }
    
    if len(post_request_params.topics) > 0:
        search_criteria['labels.topics'] = { '$in': [UUID(i) for i in post_request_params.topics] }
    
    if len(post_request_params.persons) > 0:
        search_criteria['labels.persons'] = { '$in': [UUID(i) for i in post_request_params.persons] }

    if len(post_request_params.locations) > 0:
        search_criteria['labels.locations'] = { '$in': [UUID(i) for i in post_request_params.locations] }

    return search_criteria


async def mongo(classes, request):
    sub_domain = request.url._url.split('.ibex-app.com')[0].split('//')[1]
    sub_domain = sub_domain if sub_domain in ['dev', 'un', 'isfed'] else 'dev'
    mongodb_connection_string = os.getenv(f'MONGO_CS')

    client = motor.motor_asyncio.AsyncIOMotorClient(mongodb_connection_string)
    await init_beanie(database=client.ibex, document_models=classes)


def json_responce(result):
    json_result = json.loads(json_util.dumps(result))
    return JSONResponse(content=jsonable_encoder(json_result), status_code=200)


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

    for post in posts:
        post['api_dump'] = ''

    return posts


async def modify_monitor_search_terms(postMonitor):
    # if search terms are passed, it needs to be compared to existing list and
    # and if changes are made, existing records needs to be modified
    # finding search terms in db which are no longer preseng in the post request
    # print(postMonitor)
    db_search_terms: List[SearchTerm] = await SearchTerm.find(In(SearchTerm.tags, [str(postMonitor.id)])).to_list()
    db_search_terms_to_to_remove_from_db: List[SearchTerm] = [search_term for search_term in db_search_terms if
                                                              search_term.term not in postMonitor.search_terms]
    # print('passed_search_terms:')
    # print_(postMonitor.search_terms)
    # # print('db_search_terms:')
    # print_(db_search_terms)
    # # print('db_search_terms_to_to_remove_from_db:')
    # print_(db_search_terms_to_to_remove_from_db)
    for search_term in db_search_terms_to_to_remove_from_db:
        search_term.tags = [tag for tag in search_term.tags if tag != str(postMonitor.id)]
        await search_term.save()

    # finding search terms that are not taged in db
    db_search_terms_strs: List[str] = [search_term.term for search_term in db_search_terms]
    search_terms_to_add_to_db: List[str] = [search_term for search_term in postMonitor.search_terms if
                                            search_term not in db_search_terms_strs]
    # print(f'db_search_terms_strs: {db_search_terms_strs}')
    # print('search_terms_to_add_to_db:')
    # print_(search_terms_to_add_to_db)
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
    db_accounts: List[SearchTerm] = await Account.find(In(Account.tags, [postMonitor.id])).to_list()
    account_not_in_monitor = lambda db_account: len(
        [account for account in postMonitor.accounts if account == db_account.id]) == 0
    db_accounts_terms_to_to_remove_from_db: List[Account] = [account for account in db_accounts if
                                                             account_not_in_monitor(account)]

    for account in db_accounts_terms_to_to_remove_from_db:
        account.tags = [tag for tag in account.tags if tag != postMonitor.id]
        await account.save()

    accounts_to_insert = [Account(
        title=account.title,
        platform=account.platform,
        platform_id=account.platform_id,
        tags=[str(postMonitor.id)],
        url='') for account in postMonitor.accounts if not account.id]

    if len(accounts_to_insert): await Account.insert_many(accounts_to_insert)


def collect_data_cmd(monitor_id:str, sample:bool = False):
    cmd = f'python3 /root/data-collection-and-processing/main.py --monitor_id={monitor_id} {"--sample" if sample else ""} >> celery_worker.out'
    print(f'running command for data collection: {cmd}')
    subprocess.Popen(cmd, stdout=None, stderr=None, stdin=None, close_fds=True, shell=True)


async def get_keywords_in_monitor(monitor_id):
    collect_tasks = await CollectTask.find(CollectTask.monitor_id == UUID(monitor_id)).to_list()
    getTerm = lambda collect_task: None if not collect_task.search_terms else collect_task.search_terms[0].term
    unique = set([getTerm(collect_task) for collect_task in collect_tasks])

    keywords_already_in_monitor = []
    for search_term in unique:
        if not search_term: continue
        keywords_already_in_monitor += re.split(' AND | OR | NOT ', search_term)

    return list(set(keywords_already_in_monitor))


async def get_posts_aggregated(post_request_params_aggregated: RequestPostsFiltersAggregated):
    search_criteria = await generate_search_criteria(post_request_params_aggregated.post_request_params)
    
    axisX = f"${post_request_params_aggregated.axisX}" \
        if post_request_params_aggregated.axisX in ['platform', 'author_platform_id'] \
        else f"$labels.{post_request_params_aggregated.axisX}" 

    aggregation = {}
    if post_request_params_aggregated.days is None:
        aggregation = axisX
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
        'count': {'$sum':1}
        } 
    }

    aggregations.append({'$group': {
        '_id': aggregation , 
        'count': {'$sum':1}
        } 
    })
    
    if post_request_params_aggregated.axisX not in ['platform', 'author_platform_id']:
        aggregations.append({
                '$lookup': {
                    'from': "tags",
                    'localField': f"_id{'.label' if post_request_params_aggregated.days is not None else ''}",
                    'foreignField': "_id",
                    'as': f"{post_request_params_aggregated.axisX}"
                }
            })
        aggregations.append({'$unwind': f"${post_request_params_aggregated.axisX}" })
    else:
        set_ = { '$set': {} }
        set_['$set'][post_request_params_aggregated.axisX] = '$_id'
        aggregations.append(set_)

    result = await Post.find(search_criteria)\
        .aggregate([
            *aggregations,
        ])\
        .to_list()

    return result

async def get_monitor_platforms(monitor_id: UUID) -> List[Platform]:
    collect_actions: List[CollectAction] = await CollectAction.find(CollectAction.monitor_id == monitor_id).to_list()
    platforms = set([collect_action.platform for collect_action in collect_actions])
    return platforms

async def fetch_full_monitor(monitor_id: str):    
    monitor = await Monitor.get(monitor_id)
    search_terms = await SearchTerm.find(In(SearchTerm.tags, [monitor_id])).to_list()
    accounts = await Account.find(In(SearchTerm.tags, [monitor_id])).to_list()
    collect_actions = await CollectAction.find(CollectAction.monitor_id == UUID(monitor_id)).to_list()
    platforms = set([collect_action.platform for collect_action in collect_actions])

    return { 'monitor': monitor, 'search_terms': search_terms, 'accounts': accounts, 'platforms': platforms }