from tkinter import Label
from turtle import st
from beanie import init_beanie
import motor

from bson.binary import Binary

from datetime import datetime
from uuid import UUID, uuid1
from typing import List
from ibex_models import  Post, Annotations, TextForAnnotation, Monitor, Platform, Account, SearchTerm, CollectAction, CollectTask, Labels, CollectTaskStatus
from model import RequestPostsFilters, RequestAnnotations, RequestPostsFiltersAggregated, RequestMonitor, RequestTag, RequestId, RequestAccountsSearch, RequestMonitorEdit, RequestAddTagToPost
from beanie.odm.operators.find.comparison import In

from bson import json_util, ObjectId
from bson.json_util import dumps, loads
from pydantic import Field, BaseModel, validator

import json 
import pandas as pd

from fastapi import FastAPI, Request, Depends
from fastapi.responses import JSONResponse, RedirectResponse
from fastapi.encoders import jsonable_encoder
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
import os
import subprocess

from starlette.middleware.sessions import SessionMiddleware
from authlib.integrations.starlette_client import OAuthError
from jwt_ import create_refresh_token
from jwt_ import create_token
from jwt_ import CREDENTIALS_EXCEPTION
from jwt_ import get_current_user_email

from authlib.integrations.starlette_client import OAuth
from starlette.config import Config

from app.core.datasources import collector_classes

# OAuth settings
GOOGLE_CLIENT_ID = os.environ.get('GOOGLE_CLIENT_ID') or None
GOOGLE_CLIENT_SECRET = os.environ.get('GOOGLE_CLIENT_SECRET') or None

if GOOGLE_CLIENT_ID is None or GOOGLE_CLIENT_SECRET is None:
    raise BaseException('Missing env variables')

# Set up OAuth
config_data = {'GOOGLE_CLIENT_ID': GOOGLE_CLIENT_ID, 'GOOGLE_CLIENT_SECRET': GOOGLE_CLIENT_SECRET}
starlette_config = Config(environ=config_data)
oauth = OAuth(starlette_config)
oauth.register(
    name='google',
    server_metadata_url='https://accounts.google.com/.well-known/openid-configuration',
    client_kwargs={'scope': 'openid email profile'},
)


app = FastAPI()

origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
    
app.add_middleware(SessionMiddleware, secret_key='_SECRET_KEY_')

# @staticmethod
def generate_search_criteria(post_request_params: RequestPostsFilters):
    search_criteria = {
        # 'created_at': { '$gte': post_request_params.time_interval_from, '$lte': post_request_params.time_interval_to},
    }
    
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
    mongodb_connection_string = os.getenv(f'MONGO_CS_{sub_domain.upper()}')

    client = motor.motor_asyncio.AsyncIOMotorClient(mongodb_connection_string)
    await init_beanie(database=client.ibex, document_models=classes)


def json_responce(result):
    json_result = json.loads(json_util.dumps(result))
    return JSONResponse(content=jsonable_encoder(json_result), status_code=200)


@app.post("/posts", response_description="Get list of posts", response_model=List[Post])
async def posts(request: Request, post_request_params: RequestPostsFilters, current_email: str = Depends(get_current_user_email)) -> List[Post]:
    await mongo([Post, CollectTask], request)
    search_criteria = generate_search_criteria(post_request_params)

    result = await Post.find(search_criteria)\
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

    for result_ in result:
        result_['api_dump'] = ''

    non_finalized_collect_tasks_count = await CollectTask\
        .find(CollectTask.monitor_id == post_request_params.monitor_id \
            and CollectTask.status != CollectTaskStatus.finalized)\
        .count()

    is_loading = non_finalized_collect_tasks_count > 0
    
    responce = {
        'posts': result,
        'is_loading': is_loading
    }
    return json_responce(responce)


@app.post("/download_posts", response_description="Get csv file of posts")
async def download_posts(request: Request, post_request_params:RequestPostsFilters):
    await mongo([Post], request)
    search_criteria = generate_search_criteria(post_request_params)

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

    for result_ in posts:
        result_['api_dump'] = ''
    
    posts_df = pd.DataFrame([o.__dict__ for o in posts])
    path = f'/root/frontend/build/{post_request_params.monitor_id}.csv'
    posts_df.to_csv(path, index=None)
    return JSONResponse(content=jsonable_encoder({'file_location': path}), status_code=200)
    

@app.post("/posts_aggregated", response_description="Get aggregated data for posts")#, response_model=List[Post])
async def posts_aggregated(request: Request, post_request_params_aggregated: RequestPostsFiltersAggregated, current_email: str = Depends(get_current_user_email)):
    
    await mongo([Post], request)
    
    search_criteria = generate_search_criteria(post_request_params_aggregated.post_request_params)
    
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

    # print(aggregations)
    # print(search_criteria)
    result = await Post.find(search_criteria)\
        .aggregate([
            *aggregations,
        ])\
        .to_list()

    return json_responce(result)


@app.post("/post", response_description="Get post details")
async def post(request: Request, postRequestParamsSinge: RequestId, current_email: str = Depends(get_current_user_email)) -> Post:
    await mongo([Post], request)
    id_ = loads(f'{{"$oid":"{postRequestParamsSinge.id}"}}')

    post = await Post.find_one(Post.id == id_)
    post.api_dump = {}
    return JSONResponse(content=jsonable_encoder(post), status_code=200)


@app.post("/add_tag_to_post", response_description="Updated post is returned")
async def add_tag_to_post(request: Request, requestAddTagToPost: RequestAddTagToPost, current_email: str = Depends(get_current_user_email)) -> bool:
    await mongo([Post], request)
    post = await Post.get(requestAddTagToPost.id)
    post.labels = getattr(post, 'labels', Labels())
    post.labels.manual_tags = getattr(post.labels, 'manual_tags', [])
    post.labels.manual_tags += requestAddTagToPost.tags
    await post.save()
    return True


@app.post("/create_monitor", response_description="Create monitor")
async def create_monitor(request: Request, postMonitor: RequestMonitor, current_email: str = Depends(get_current_user_email)) -> Monitor:
    await mongo([Monitor, Account, SearchTerm, CollectAction], request)

    monitor = Monitor(
        title=postMonitor.title, 
        descr=postMonitor.descr, 
        collect_actions = [], 
        date_from=postMonitor.date_from, 
        date_to=postMonitor.date_to
    )
    # print(monitor.id, type(monitor.id), type(str(monitor.id)))
    search_terms = []
    if postMonitor.search_terms:
        search_terms = [SearchTerm(
                term=search_term, 
                tags=[str(monitor.id)]
            ) for search_term in postMonitor.search_terms]
    
    accounts = []
    if postMonitor.accounts:
        accounts = [Account(
                title = account.title, 
                platform = account.platform, 
                platform_id = account.platform_id, 
                tags = [str(monitor.id)],
                url=''
            ) for account in postMonitor.accounts]
    
    platforms = postMonitor.platforms if postMonitor.platforms and len(postMonitor.platforms) else [account.platform for account in postMonitor.accounts]
    
    # Create single CollectAction per platform
    collect_actions = [CollectAction(
            monitor_id = monitor.id,
            platform = platform, 
            search_term_tags = [str(monitor.id)], 
            account_tags=[str(monitor.id)],
            tags = [],
        ) for platform in platforms]
    
    monitor.collect_actions = [collect_action.id for collect_action in collect_actions]

    if len(search_terms): await SearchTerm.insert_many(search_terms)
    if len(accounts): await Account.insert_many(accounts)
    await CollectAction.insert_many(collect_actions)
    await monitor.save()

    return monitor


@app.post("/update_monitor", response_description="Create monitor")
async def update_monitor(request: Request, postMonitor: RequestMonitorEdit) -> Monitor:
    # the method modifies the monitor in databes and related records
    await mongo([Monitor, Account, SearchTerm, CollectAction, Post, CollectTask], request)
    print(type(postMonitor.id))
    await CollectTask.find(CollectTask.monitor_id == postMonitor.id).delete()
    await Post.find(In(Post.monitor_ids, [postMonitor.id])).delete()
    
    monitor = await Monitor.get(postMonitor.id)

    # if date_from and date_to exists in postMonitor, it is updated
    if postMonitor.date_from: monitor.date_from = postMonitor.date_from
    if postMonitor.date_to: monitor.date_to = postMonitor.date_to
    
    if postMonitor.search_terms:
        await modify_monitor_search_terms(postMonitor)
    if postMonitor.accounts:
        await modify_monitor_accounts(postMonitor)

    await monitor.save()
    collect_sample_cmd(postMonitor.id)
    # if platforms are passed, it needs to be compared to existing list and
    # and if changes are made, existing records needs to be modified
    # platforms: Optional[List[Platform]]

    # if languages are passed, it needs to be compared to existing list and
    # and if changes are made, existing records needs to be modified
    # languages: Optional[List[str]]

def print_(lll):
    print([i if type(i) == str else i.term for i in lll])

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
    # print('db_search_terms:')
    # print_(db_search_terms)
    # print('db_search_terms_to_to_remove_from_db:')
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


@app.post("/collect_sample", response_description="Run sample collection pipeline")
async def collect_sample(request: Request, monitor_id: RequestId, current_email: str = Depends(get_current_user_email)):
    collect_sample_cmd(monitor_id.id)

def collect_sample_cmd(monitor_id:str):
    cmd = f'python3 /root/data-collection-and-processing/main.py --monitor_id={monitor_id} --sample=True >> api.out'
    subprocess.Popen(cmd, stdout=None, stderr=None, stdin=None, close_fds=True, shell=True)


@app.post("/get_hits_count", response_description="Get amount of post for monitor")
async def get_hits_count(request: Request, postRequestParamsSinge: RequestId, current_email: str = Depends(get_current_user_email)):
    await mongo([CollectTask], request)

    collect_tasks = await CollectTask.find(CollectTask.monitor_id == UUID(postRequestParamsSinge.id)).to_list()
    # counts = {} 
    # for platform in Platform:
    #     counts[platform] = sum([collect_task.hits_count or 0 for collect_task in collect_tasks if collect_task.platform == platform])
    from itertools import groupby
    import numbers

    getTerm = lambda collect_task: collect_task.search_terms[0].term
    getTottal = lambda search_term_counts: sum([i for i in search_term_counts.values() if isinstance(i,  numbers.Number)])

    grouped = {}
    for collect_task in collect_tasks:
        term = getTerm(collect_task)
        if term not in grouped: grouped[term] = []
        grouped[term].append(collect_task) 

    terms_with_counts = { 'search_terms': [] }
    for search_term, collect_tasks in grouped.items():
        term_counts = {}
        term_counts['search_term'] = search_term
        for collect_task in collect_tasks:
            term_counts[collect_task.platform] = collect_task.hits_count
        terms_with_counts['search_terms'].append(term_counts)

    terms_with_counts['search_terms'] = sorted(terms_with_counts['search_terms'], key=getTottal)
    return JSONResponse(content=jsonable_encoder(terms_with_counts), status_code=200)

    
@app.post("/get_monitor", response_description="Get monitor")
async def search_account(request: Request, monitor_id: RequestId, current_email: str = Depends(get_current_user_email)):
    await mongo([Monitor, SearchTerm, Account], request)
    monitor = await Monitor.get(monitor_id.id)
    search_terms = await SearchTerm.find(In(SearchTerm.tags, [monitor_id.id])).to_list()
    accounts = await Account.find(In(SearchTerm.tags, [monitor_id.id])).to_list()
    return { 'monitor': monitor, 'search_terms': search_terms, 'accounts': accounts }


@app.post("/get_monitors", response_description="Get monitors")
async def get_monitors(request: Request, post_tag: RequestTag, current_email: str = Depends(get_current_user_email)) -> Monitor:
    await mongo([Monitor], request)
    if post_tag.tag == '*':
        monitor = await Monitor.find().to_list()
    else:
        monitor = await Monitor.find(In(Monitor.tags, post_tag.tag)).to_list()

    return JSONResponse(content=jsonable_encoder(monitor), status_code=200)


@app.post("/search_account", response_description="Search accounts by string across all platforms")
async def search_account(request: Request, search_accounts: RequestAccountsSearch, current_email: str = Depends(get_current_user_email)):
    await mongo([Account], request)
    accounts: List[Account] = []
    for platform in collector_classes:
        # if platform in [Platform.facebook]: continue
        data_source = collector_classes[platform]()
        accounts_from_platform: List[Account] = await data_source.get_accounts(search_accounts.substring)
        accounts += accounts_from_platform[:3]
    for account in accounts:
        account.label = account.title
        account.icon = account.platform
    return JSONResponse(content=jsonable_encoder(accounts), status_code=200)


@app.post("/save_and_next", response_description="Save the annotations for the text and return new text for annotation", response_model=TextForAnnotation)
async def save_and_next(request: Request, request_annotations: RequestAnnotations, current_email: str = Depends(get_current_user_email)) -> TextForAnnotation:
    await mongo([Annotations, TextForAnnotation], request)
    
    if request_annotations.text_id:
        annotations = Annotations(text_id = request_annotations.text_id, user_mail = current_email, annotations = request_annotations.annotations)
        # print('inserting annotation', annotations)
        await annotations.insert()

    already_annotated = await Annotations.aggregate([
        {"$match": { "user_mail": { "$eq": current_email }}}
    ]).to_list()

    # print('already', len(already_annotated))
    
    annotated_text_ids = [annotations["text_id"]  for annotations in already_annotated]

    text_for_annotation = await TextForAnnotation.aggregate([
        {"$match": { "_id": { "$nin": annotated_text_ids }}},
        {
            "$lookup":
                {
                    "from": "annotations",
                    "localField": "_id",
                    "foreignField": "text_id",
                    "as": "annotations_"
                }
        }, 
        {
            "$project":
            {
                "annotations_": { "$cond" : [ { "$eq" : [ "$annotations_", [] ] }, [ { "tag_id": 0, "label": "", "words": [], "labelGrup": ""} ], '$annotations_' ] }
            }
        },
        {"$unwind": "$annotations_"},
        {"$group" : {"_id":"$_id", "count":{"$sum":1}}},
        {"$match": { "count": { "$lt": 4 }}},
        {"$sample": {"size": 1}},
        {
            "$lookup":
                {
                    "from": "text_for_annotation",
                    "localField": "_id",
                    "foreignField": "_id",
                    "as": "text"
                }
        },
        {"$unwind": "$text"},
    ]).to_list()
    
    # print(len(text_for_annotation))
    # print(text_for_annotation)
    if len(text_for_annotation) == 0:
        return TextForAnnotation(id=uuid1(), words=[])
    
    text_for_annotation = TextForAnnotation(id=text_for_annotation[0]["_id"], post_id = text_for_annotation[0]["text"]["post_id"], words=text_for_annotation[0]["text"]["words"])
    return text_for_annotation


@app.get('/login')
async def login(request: Request):
    env = 'dev' if 'localhost' in request.headers['referer'] else 'prod'
    host = 'https://dev.ibex-app.com/' if env == 'dev' else request.headers['referer'].rstrip('login')
    redirect_uri = f'{host}api/token?env={env}'
    redirect = await oauth.google.authorize_redirect(request, redirect_uri)
    
    # print(0, redirect_uri)

    # print(1111, redirect.__dict__)
    return redirect


@app.route('/token')
async def auth(request: Request):
    sub_domain = request.url._url.split('.ibex-app.com')[0].split('//')[1]
    # print(222, request.session)
    # print(333, request.__dict__)
    # print(444, request.url._url)
    try:
        access_token = await oauth.google.authorize_access_token(request)
    except OAuthError as err:
        print(str(err))
        raise CREDENTIALS_EXCEPTION

    user_data = await oauth.google.parse_id_token(access_token, access_token['userinfo']['nonce'])
    valid_accounts = os.environ.get('VALID_ACCOUNTS').split('__SEP__')
    if user_data['email'] in valid_accounts or sub_domain == 'tag':
        obj_ = {
            'result': True,
            'access_token': create_token(user_data['email']).decode("utf-8") ,
            'refresh_token': create_refresh_token(user_data['email']).decode("utf-8") ,
        }
        return_url = 'http://localhost:3000' if request.query_params['env'] == 'dev' else f'https://{sub_domain}.ibex-app.com'
        return RedirectResponse(url=f"{return_url}?access_token={obj_['access_token']}&user={user_data['email']}")
    print('no email')
    raise CREDENTIALS_EXCEPTION

# create_monitor, update_monitor, get_monitors, collect_sample, get_hits_count, search_account

# curl -X 'POST' \
#   'http://161.35.73.100:8888/create_monitor' \
#   -H 'accept: application/json' \
#   -H 'Content-Type: application/json' \
#   -d '{  "title": "Testing Monitor api", "descr": "descr", "date_from": "2022-02-01T00:00:00.000Z", "search_terms": ["?????????????????????"], "platforms": ["facebook"], "accounts": [{"title": "title",  "platform_id": "platform_id", "platform": "facebook"}] }'


# curl -X 'POST' \
#   'http://161.35.73.100:8888/get_monitors' \
#   -H 'accept: application/json' \
#   -H 'Content-Type: application/json' \
#   -d '{ "tag": "*"}'

# curl -X 'POST' \
#   'http://161.35.73.100:8888/collect_sample' \
#   -H 'accept: application/json' \
#   -H 'Content-Type: application/json' \
#   -d '{ "id": "f06a6a87-7fb8-4a53-8c46-90accd89aa8c"}'