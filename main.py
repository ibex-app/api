from platform import platform
from tkinter import Label

from itertools import chain
from urllib import response
from bson.binary import Binary
import math
from datetime import datetime
from uuid import UUID, uuid1
from typing import List
from ibex_models import  Post, Annotations, TextForAnnotation, Monitor, Platform, Account, SearchTerm, CollectAction, CollectTask, Labels, CollectTaskStatus
from model import RequestPostsFilters, RequestAnnotations, RequestPostsFiltersAggregated, RequestMonitor, RequestTag, RequestId, RequestAccountsSearch, RequestMonitorEdit, RequestAddTagToPost
from beanie.odm.operators.find.comparison import In
import numbers


from bson.json_util import dumps, loads

import pandas as pd

from fastapi import FastAPI, Request, Depends, HTTPException
from fastapi.responses import JSONResponse, RedirectResponse
from fastapi.encoders import jsonable_encoder
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
import os

from starlette.middleware.sessions import SessionMiddleware
from authlib.integrations.starlette_client import OAuthError
from jwt_ import create_refresh_token
from jwt_ import create_token
from jwt_ import CREDENTIALS_EXCEPTION
from jwt_ import get_current_user_email

from authlib.integrations.starlette_client import OAuth
from starlette.config import Config

from app.core.datasources import collector_classes

from sklearn.feature_extraction.text import TfidfVectorizer
import nltk
from nltk.corpus import stopwords
from asyncio import gather
from utils import ( modify_monitor_search_terms, 
                    modify_monitor_accounts, 
                    collect_data_cmd, 
                    terminate_monitor_tasks, 
                    get_keywords_in_monitor, 
                    get_keywords_in_monitor, 
                    generate_search_criteria, 
                    mongo, 
                    json_responce, 
                    get_posts,
                    get_keywords_in_monitor,
                    get_posts_aggregated,
                    fetch_full_monitor,
                    get_monitor_platforms)

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

# POSTS

@app.post("/posts", response_description="Get list of posts", response_model=List[Post])
async def posts(request: Request, post_request_params: RequestPostsFilters, current_email: str = Depends(get_current_user_email)) -> List[Post]:
    await mongo([Post, CollectTask, SearchTerm, CollectAction], request)
    
    if post_request_params.shuffle:
        platforms = await get_monitor_platforms(UUID(post_request_params.monitor_id))
        post_request_params.start_index = math.ceil(post_request_params.start_index/len(platforms))
        post_request_params.count = math.ceil(post_request_params.count/len(platforms))
        posts = []
        for platform in platforms:
            post_request_params.platform = [platform]
            posts += await get_posts(post_request_params)
    else: 
        posts = await get_posts(post_request_params)
    
    if post_request_params.monitor_id:
        non_finalized_collect_tasks_count = await CollectTask\
            .find(CollectTask.monitor_id == UUID(post_request_params.monitor_id),
                CollectTask.status != CollectTaskStatus.finalized)\
            .count()

        collect_tasks_count = await CollectTask\
            .find(CollectTask.monitor_id == UUID(post_request_params.monitor_id)).count()

        is_loading = collect_tasks_count == 0 or non_finalized_collect_tasks_count > 0
    else:
        is_loading = False
    responce = {
        'posts': posts,
        'is_loading': is_loading
    }
    return json_responce(responce)


@app.post("/download_posts", response_description="Get csv file of posts")
async def download_posts(request: Request, post_request_params:RequestPostsFilters):
    await mongo([Post, CollectTask, SearchTerm, CollectAction], request)
    posts = await get_posts(post_request_params)
    
    for post in posts:
        del post['api_dump']
        # TODO add labels to downloadable df
        # for label in post['labels']:
        #     post[f'label_{label}'] = post['labels'][label]
        del post['labels']
        for score in post['scores']:
            post[f'score_{score}'] = post['scores'][score]
        del post['scores']

    posts_df = pd.DataFrame(posts)
    filename = f'{post_request_params.monitor_id}_{datetime.now().strftime("%s")}.csv'
    path = f'/root/static/{filename}'
    posts_df.to_csv(path, index=None)
    return JSONResponse(content=jsonable_encoder({'file_location': f'https://static.ibex-app.com/{filename}'}), status_code=200)
    

@app.post("/posts_aggregated", response_description="Get aggregated data for posts")#, response_model=List[Post])
async def posts_aggregated(request: Request, post_request_params_aggregated: RequestPostsFiltersAggregated, current_email: str = Depends(get_current_user_email)):
    await mongo([Post], request)
    result = await get_posts_aggregated(post_request_params_aggregated)

    return json_responce(result)


@app.post("/download_posts_aggregated", response_description="Get aggregated data for posts")
async def download_posts_aggregated(request: Request, post_request_params_aggregated: RequestPostsFiltersAggregated, current_email: str = Depends(get_current_user_email)):
    await mongo([Post], request)
    result = await get_posts_aggregated(post_request_params_aggregated)

    posts_df = pd.DataFrame(result)
    filename = f'{post_request_params_aggregated.post_request_params.monitor_id}_aggregated_{datetime.now().strftime("%s")}.csv'
    path = f'/root/static/{filename}'
    posts_df.to_csv(path, index=None)

    return JSONResponse(content=jsonable_encoder({'file_location': f'https://static.ibex-app.com/{filename}'}), status_code=200)


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

# MONITORS


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

    print(f'Inserting {len(collect_actions)} collect actions...')
    print(f'Monitor id {monitor.id}')
    
    await CollectAction.insert_many(collect_actions)
    await monitor.save()

    return monitor

@app.post("/update_monitor", response_description="Create monitor")
async def update_monitor(request: Request, postMonitor: RequestMonitorEdit) -> Monitor:

    # if platforms are passed, it needs to be compared to existing list and
    # and if changes are made, existing records needs to be modified
    # platforms: Optional[List[Platform]]

    # if languages are passed, it needs to be compared to existing list and
    # and if changes are made, existing records needs to be modified
    # languages: Optional[List[str]]

    # the method modifies the monitor in databes and related records
    await mongo([Monitor, Account, SearchTerm, CollectAction, Post, CollectTask], request)

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
    terminate_monitor_tasks(monitor.id)
    collect_data_cmd(postMonitor.id, True)
    
@app.post("/get_monitor", response_description="Get monitor")
async def get_monitor(request: Request, monitor_id: RequestId, current_email: str = Depends(get_current_user_email)):
    await mongo([Monitor, SearchTerm, Account, CollectAction], request)
    full_monitor = await fetch_full_monitor(monitor_id.id)
    return full_monitor

@app.post("/get_monitors", response_description="Get monitors")
async def get_monitors(request: Request, post_tag: RequestTag, current_email: str = Depends(get_current_user_email)) -> Monitor:
    await mongo([Monitor], request)
    if post_tag.tag == '*':
        monitor = await Monitor.find().to_list()
    else:
        monitor = await Monitor.find(In(Monitor.tags, post_tag.tag)).to_list()

    return JSONResponse(content=jsonable_encoder(monitor), status_code=200)

@app.post("/monitor_progress", response_description="Get monitor")
async def monitor_progress(request: Request, monitor_id: RequestId, current_email: str = Depends(get_current_user_email)):
    await mongo([Post, CollectTask, SearchTerm, Account, CollectAction], request)
    status_result = []
    platforms = await get_monitor_platforms(UUID(monitor_id.id))
    for platform in platforms:
        platform_progress = dict(
            tasks_count=await CollectTask.find(
                CollectTask.monitor_id == UUID(monitor_id.id),
                In(CollectTask.platform, [platform])
            ).count(),
            finalized_collect_tasks_count=await CollectTask.find(
                CollectTask.monitor_id == UUID(monitor_id.id), CollectTask.status == CollectTaskStatus.finalized
                , In(CollectTask.platform, [platform])).count(),
            posts_count=await Post.find(
                In(Post.monitor_ids, [UUID(monitor_id.id)]),
                Post.platform == platform).count())
        platform_progress['platform'] = platform    
        platform_progress["time_estimate"] = (int(platform_progress["tasks_count"])
                                      - int(platform_progress["finalized_collect_tasks_count"])) * 4
        status_result.append(platform_progress)
    return json_responce(status_result)


# COLLECT

@app.post("/run_data_collection", response_description="Run data collection")
async def run_data_collection(request: Request, monitor_id: RequestId, current_email: str = Depends(get_current_user_email)) -> Monitor:
    await mongo([Post, Monitor,CollectAction, CollectTask, SearchTerm, Account], request)
    collect_tasks = await CollectTask.find(CollectTask.monitor_id == UUID(monitor_id.id), CollectTask.get_hits_count == True).to_list()
    
    out_of_range = []
    for collect_task in collect_tasks:
        if collect_task.hits_count and collect_task.hits_count > 10000:
            out_of_range.append(collect_task)

    if len(out_of_range) > 0:
        raise HTTPException(status_code=412, detail=jsonable_encoder({'out_of_limit': out_of_range}))
        # return {'out_of_limit': [out_of_range]}

    await Post.find(In(Post.monitor_ids, [UUID(monitor_id.id)])).delete()
    await CollectTask.find(CollectTask.monitor_id == UUID(monitor_id.id)).delete()
    terminate_monitor_tasks(UUID(monitor_id.id))
    collect_data_cmd(monitor_id.id)

    full_monitor = await fetch_full_monitor(monitor_id.id)
    return full_monitor


@app.post("/collect_sample", response_description="Run sample collection pipeline")
async def collect_sample(request: Request, monitor_id: RequestId, current_email: str = Depends(get_current_user_email)):
    collect_data_cmd(monitor_id.id, True)


# TAXONOMY TOOL 

@app.post("/get_hits_count", response_description="Get amount of post for monitor")
async def get_hits_count(request: Request, postRequestParamsSinge: RequestId, current_email: str = Depends(get_current_user_email)):
    await mongo([CollectTask, SearchTerm, Account], request)

    collect_tasks = await CollectTask.find(CollectTask.monitor_id == UUID(postRequestParamsSinge.id), CollectTask.get_hits_count == True).to_list()
    # counts = {} 
    # for platform in Platform:
    #     counts[platform] = sum([collect_task.hits_count or 0 for collect_task in collect_tasks if collect_task.platform == platform])
    

    # getTerm = lambda collect_task: collect_task.search_terms[0].term
    
    def get_name(collect_task):
        if collect_task.search_terms and len(collect_task.search_terms):
            return collect_task.search_terms[0].term
        elif collect_task.accounts and len(collect_task.accounts):
            return collect_task.accounts[0].title

    getTottal = lambda search_term_counts: sum([i for i in search_term_counts.values() if isinstance(i,  numbers.Number)])
    
    grouped = {}
    for collect_task in collect_tasks:
        term = get_name(collect_task)
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
    

@app.post("/search_account", response_description="Search accounts by string across all platforms")
async def search_account(request: Request, search_accounts: RequestAccountsSearch, current_email: str = Depends(get_current_user_email)):
    await mongo([Account], request)
    accounts: List[Account] = []
    
    methods = []
    for platform in collector_classes:
        data_source = collector_classes[platform]()
        methods.append(data_source.get_accounts)

    accounts_from_platforms = await gather(*[method(search_accounts.substring) for method in methods])

    responce = []
    for account in chain.from_iterable([accounts_from_platform[:3] for accounts_from_platform in accounts_from_platforms]):
        responce.append({
            'label': account.title,
            'icon': account.platform,
            'platform_id': account.platform_id,
            'platform': account.platform
        })
    return JSONResponse(content=jsonable_encoder(responce), status_code=200)


nltk.download('stopwords')
stop_words = set(stopwords.words('russian'))
e_stop_words = set(stopwords.words('english'))
stop_words.update(e_stop_words)


@app.post("/recommendations", response_description="Get monitor")
async def recommendations(request: Request, monitor_id: RequestId, current_email: str = Depends(get_current_user_email)):
    await mongo([Monitor, Post], request)
    """
    :param: monitor_ids: Are ids taken from database. monitor_ids[0] is the id we want to search words for.
    :return: Returns top 10 frequent words.
    """
    await mongo([Monitor, Post], request)

    monitor_posts = await Post.find({ 'monitor_ids': {'$nin': [UUID(monitor_id.id)] }}).aggregate([{ '$sample': { 'size': 1500 } } ]).to_list()
    sample_size = 500 if len(monitor_posts) < 500 else len(monitor_posts)
    # sample_size = 100
    other_posts = await Post.find({ 'monitor_ids': {'$nin': [UUID(monitor_id.id)] }}).aggregate([{ '$sample': { 'size': sample_size } } ]).to_list()
    
    docs = [' '.join([post['text'] for post in monitor_posts]), ' '.join([post['text'] for post in other_posts])]
    keywords_already_in_monitor = await get_keywords_in_monitor(monitor_id.id)
    
    for stop_word in keywords_already_in_monitor + ["https","com","news","www","https www","twitter","youtube","facebook", "ly","bit", "bit ly", "instagram", "channel", "http", "subscribe"]:
        stop_words.add(stop_word.lower())

    vectorizer = TfidfVectorizer(stop_words=stop_words, ngram_range=(1, 2))

    response = vectorizer.fit_transform(docs)
    df_tfidf_sklearn = pd.DataFrame(response.toarray(), columns=vectorizer.get_feature_names_out())
    monitor_tfidfs = df_tfidf_sklearn.iloc[0]
    tokens = df_tfidf_sklearn.columns

    tokens_with_tfidfs = []
    for tf_idf, token in zip(monitor_tfidfs, tokens):
        tokens_with_tfidfs.append((tf_idf, token))
    words = sorted(tokens_with_tfidfs, reverse=True)[:10]
    words = [{ 'word': word[1], 'score': word[0]} for word in words]
    
    return JSONResponse(content=jsonable_encoder(words), status_code=200)

# TAGGING

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


# AUTH

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
