from tkinter import Label
from itertools import chain
from functools import reduce
from urllib import response
from bson.binary import Binary
import math
from datetime import datetime
from uuid import UUID, uuid1
from typing import List
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
from app.core.populate_collectors import sampling_tasks_match

from asyncio import gather
from sklearn.feature_extraction.text import TfidfVectorizer
import nltk
from nltk.corpus import stopwords
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
                    update_collect_actions,
                    delete_out_of_monitor_posts,
                    remove_spec_chars,
                    get_monitor_platfroms_with_posts)
from ibex_models import  (MonitorStatus,
                         Post,
                         Annotations,
                         TextForAnnotation,
                         Monitor,
                         Platform,
                         Account,
                         SearchTerm,
                         CollectAction,
                         CollectTask,
                         Labels,
                         CollectTaskStatus) 
from model import (RequestPostsFilters,
                   RequestAnnotations,
                   RequestPostsFiltersAggregated,
                   RequestMonitor,
                   RequestTag,
                   RequestId,
                   RequestSearchTerm,
                   RequestAccountsSearch,
                   RequestMonitorEdit,
                   RequestAddTagToPost)
from stopwords import low_resource_stopwords
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
    await mongo([Monitor, Post, CollectTask, SearchTerm, CollectAction, Account], request)
    
    monitor = await Monitor.get(UUID(post_request_params.monitor_id))
    platforms = await get_monitor_platfroms_with_posts(post_request_params)
    if len(platforms) == 0:
        posts = []
    elif post_request_params.shuffle:
        if post_request_params.platform and len(post_request_params.platform):
            platforms = list(set(post_request_params.platform) & set(platforms))
        post_request_params.start_index = math.ceil(post_request_params.start_index/len(platforms))
        post_request_params.count = math.ceil(post_request_params.count/len(platforms))
        posts = []
        for platform in platforms:
            post_request_params.platform = [platform if type(platform) == str else platform.value]
            posts += await get_posts(post_request_params)
    else: 
        posts = await get_posts(post_request_params)
    
    is_loading = False

    if post_request_params.monitor_id:
        collect_tasks = await CollectTask.find(CollectTask.monitor_id == UUID(post_request_params.monitor_id)).to_list()
        if len(collect_tasks) == 0 or monitor.status <= MonitorStatus.sampling:
            is_loading = True
        else:
            is_loading = not all([_.status == CollectTaskStatus.finalized or _.status == CollectTaskStatus.failed for _ in collect_tasks])

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
    
    filename = f'{post_request_params_aggregated.post_request_params.monitor_id}_aggregated_{post_request_params_aggregated.axisX}_{post_request_params_aggregated.axisY}_{datetime.now().strftime("%s")}.csv'
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
    if postMonitor.search_terms and len(postMonitor.search_terms):
        postMonitor.search_terms = [remove_spec_chars(_) for _ in postMonitor.search_terms]
    platforms:List[Platform] = postMonitor.platforms or list(set([_.platform for _ in postMonitor.accounts])) 

    monitor = Monitor(
        title=postMonitor.title, 
        descr=postMonitor.descr, 
        collect_actions = [], 
        date_from=postMonitor.date_from, 
        date_to=postMonitor.date_to,
        platforms=platforms
    )
    # print(monitor.id, type(monitor.id), type(str(monitor.id)))
    search_terms = []
    if postMonitor.search_terms:
        for search_term in postMonitor.search_terms:
            existing_search_term = await SearchTerm.find(SearchTerm.term == search_term).limit(1).to_list()
            if existing_search_term and len(existing_search_term): 
                existing_search_term[0].tags.append(str(monitor.id))
                await existing_search_term[0].save()
            else:
                search_terms.append(SearchTerm( term=search_term, 
                                                tags=[str(monitor.id)] ))
    
    accounts = []
    if postMonitor.accounts:
        for account in postMonitor.accounts:
            existing_accounts = await Account.find(Account.platform == account.platform, Account.platform_id == account.platform_id).to_list()
            if len(existing_accounts): 
                existing_accounts[0].tags.append(str(monitor.id))
                await existing_accounts.save()
            else:
                accounts.append(Account(
                    title = account.title, 
                    platform = account.platform, 
                    platform_id = account.platform_id, 
                    tags = [str(monitor.id)],
                    url=''
                ))

    
    await update_collect_actions(monitor)

    # monitor.collect_actions = [collect_action.id for collect_action in collect_actions]

    if len(search_terms): await SearchTerm.insert_many(search_terms)
    if len(accounts): await Account.insert_many(accounts)

    print(f'Monitor id {monitor.id}')
    
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
    # if not len(postMonitor.accounts) and not len(postMonitor.search_terms):
    #     raise HTTPException(detail="no accounts and search terms passed to update method")

    await mongo([Monitor, Account, SearchTerm, CollectAction, Post, CollectTask], request)
    terminate_monitor_tasks(postMonitor.id)

    await CollectTask.find( CollectTask.monitor_id == postMonitor.id,
                            CollectTask.status != CollectTaskStatus.finalized).delete()

    hits_count_tasks = await CollectTask.find( CollectTask.monitor_id == postMonitor.id, 
                                                CollectTask.get_hits_count == True).to_list()
    if postMonitor.search_terms and len(postMonitor.search_terms):
        postMonitor.search_terms = [_ if _.id else RequestSearchTerm(term=remove_spec_chars(_.term)) for _ in postMonitor.search_terms]

    for hits_count_task in hits_count_tasks:
        if ((hits_count_task.search_terms and 
            len(hits_count_task.search_terms) and 
            hits_count_task.search_terms[0].term not in [_.term for _ in postMonitor.search_terms]) or 
            (hits_count_task.accounts and 
            len(hits_count_task.accounts) and 
            hits_count_task.accounts[0].platform_id not in [_.platform_id for _ in postMonitor.accounts])):
            print('deleteing hits_count_task', hits_count_task)
            await hits_count_task.delete()

    
    monitor = await Monitor.get(postMonitor.id)
    monitor.status = MonitorStatus.sampling
    # if date_from and date_to exists in postMonitor, it is updated
    if postMonitor.date_from: monitor.date_from = postMonitor.date_from
    if postMonitor.date_to: monitor.date_to = postMonitor.date_to
    
    if postMonitor.platforms:
        monitor.platforms = postMonitor.platforms
    elif postMonitor.accounts and len(postMonitor.accounts):
        monitor.platforms = list(set([_.platform for _ in postMonitor.accounts]))

    await monitor.save()
    updated = False
    if postMonitor.search_terms:
        updated = await modify_monitor_search_terms(postMonitor)
    if postMonitor.accounts:
        updated = await modify_monitor_accounts(postMonitor)
    
    await delete_out_of_monitor_posts(postMonitor.id, request)
    await update_collect_actions(monitor)

    if updated or True:
        collect_data_cmd(postMonitor.id, True)

    
    
@app.post("/clone_monitor", response_description="Get monitor")
async def clone_monitor(request: Request, monitor_id: RequestId, current_email: str = Depends(get_current_user_email)):
    await mongo([Monitor, SearchTerm, Account, CollectAction], request)
    full_monitor = await fetch_full_monitor(monitor_id.id)
    return full_monitor

@app.post("/delete_monitor", response_description="Get monitor")
async def delete_monitor(request: Request, monitor_id: RequestId, current_email: str = Depends(get_current_user_email)):
    await mongo([Monitor, SearchTerm, Account, CollectAction], request)
    terminate_monitor_tasks(monitor_id.id)
    monitor = await Monitor.get(monitor_id.id)
    await monitor.delete()

@app.post("/get_monitor", response_description="Get monitor")
async def get_monitor(request: Request, monitor_id: RequestId, current_email: str = Depends(get_current_user_email)):
    await mongo([Monitor, SearchTerm, Account, CollectAction], request)
    monitor = await fetch_full_monitor(monitor_id.id)
    return JSONResponse(content=jsonable_encoder(monitor['full_monitor']), status_code=200)
    
    

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
    monitor = await Monitor.get(monitor_id.id)
    
    for platform in monitor.platforms:
        platform_progress = dict(
            tasks_count=await CollectTask.find(
                CollectTask.monitor_id == UUID(monitor_id.id),
                In(CollectTask.platform, [platform]),
                CollectTask.get_hits_count != True
            ).count(),
            finalized_collect_tasks_count=await CollectTask.find(
                CollectTask.monitor_id == UUID(monitor_id.id), 
                CollectTask.status == CollectTaskStatus.finalized, 
                In(CollectTask.platform, [platform]),
                CollectTask.get_hits_count != True).count(),
            failed_collect_tasks_count=await CollectTask.find(
                CollectTask.monitor_id == UUID(monitor_id.id), 
                CollectTask.status == CollectTaskStatus.failed, 
                In(CollectTask.platform, [platform]),
                CollectTask.get_hits_count != True).count(),
            posts_count=await Post.find(
                In(Post.monitor_ids, [UUID(monitor_id.id)]),
                Post.platform == platform).count())
        platform_progress['platform'] = platform    
        platform_progress["time_estimate"] = (int(platform_progress["tasks_count"])
                                      - int(platform_progress["finalized_collect_tasks_count"])) * 32
        status_result.append(platform_progress)
    return json_responce(status_result)


# COLLECT

@app.post("/run_data_collection", response_description="Run data collection")
async def run_data_collection(request: Request, monitor_id: RequestId, current_email: str = Depends(get_current_user_email)) -> Monitor:
    await mongo([Post, Monitor,CollectAction, CollectTask, SearchTerm, Account], request)
    
    full_monitor = await fetch_full_monitor(monitor_id.id)
    monitor = full_monitor['db_monitor']
    if monitor.status < MonitorStatus.collecting:
        terminate_monitor_tasks(UUID(monitor_id.id))
        print('[run_data_collection], status is less then collecting', monitor.status)
        
        collect_tasks = await CollectTask.find(CollectTask.monitor_id == UUID(monitor_id.id), CollectTask.get_hits_count == True).to_list()
        out_of_range = []
        for collect_task in collect_tasks:
            if collect_task.hits_count and collect_task.hits_count > 10000:
                out_of_range.append(collect_task)

        if len(out_of_range) > 0:
            raise HTTPException(status_code=412, detail=jsonable_encoder({'out_of_limit': out_of_range}))
            # return {'out_of_limit': [out_of_range]}

        await Post.find(In(Post.monitor_ids, [UUID(monitor_id.id)])).delete()
        await CollectTask.find(CollectTask.monitor_id == UUID(monitor_id.id), CollectTask.get_hits_count != True).delete()
        
        collect_data_cmd(monitor_id.id)
        
        monitor.status = MonitorStatus.collecting
        await monitor.save()
        
    return JSONResponse(content=jsonable_encoder(full_monitor['full_monitor']), status_code=200)


@app.post("/collect_sample", response_description="Run sample collection pipeline")
async def collect_sample(request: Request, monitor_id: RequestId, current_email: str = Depends(get_current_user_email)):
    collect_data_cmd(monitor_id.id, True)


# TAXONOMY TOOL 
def compare(hits_count_in_results, collect_task):
    search_term_or_account_1 = hits_count_in_results['item']
    search_term_or_account_2 = collect_task.accounts[0] if result['type'] == 'accounts' else collect_task.search_terms[0]
    if type(search_term_or_account_1) == SearchTerm:
        return search_term_or_account_1.term == search_term_or_account_2.term
    else:
        return search_term_or_account_1.platform == search_term_or_account_2.platform and search_term_or_account_1.platform_id == search_term_or_account_2.platform_id
        
@app.post("/get_hits_count", response_description="Get amount of post for monitor")
async def get_hits_count(request: Request, postRequestParamsSinge: RequestId, current_email: str = Depends(get_current_user_email)):
    await mongo([Monitor, CollectTask, SearchTerm, Account], request)

    all_collect_tasks = await CollectTask.find(CollectTask.monitor_id == UUID(postRequestParamsSinge.id), CollectTask.get_hits_count == True).to_list()
    collect_tasks = [_ for _ in all_collect_tasks if _.hits_count or _.hits_count == 0 ]
    
    getTottal = lambda search_term_counts: 0 - sum([i for i in search_term_counts.values() if isinstance(i,  numbers.Number)])
    
    if not len(collect_tasks): return {
        'is_loading': True,
        'data': []
    }
    
    monitor = await Monitor.get(UUID(postRequestParamsSinge.id))
    result = {
        'is_loading': monitor.status <= MonitorStatus.sampling,
        'data': [],
        'type': 'accounts' if collect_tasks[0].accounts and len(collect_tasks[0].accounts) > 0 else 'search_terms' #'account and search_term'
    }
    
    if result['type'] == 'accounts':
        db_items: List[Account] = await Account.find(In(Account.tags, [str(postRequestParamsSinge.id)])).to_list()
    else:
        db_items: List[SearchTerm] = await SearchTerm.find(In(SearchTerm.tags, [str(postRequestParamsSinge.id)])).to_list()
    
    # print('db_items', len(db_items))
    for db_item in db_items:
        hits_count = {}
        hits_count['id'] = db_item.id
        if result['type'] == 'accounts':
            hits_count['title'] = db_item.title
            hits_count['platform'] = db_item.platform
            hits_count['platform_id'] = db_item.platform_id
            hits_count['url'] = db_item.url
            hits_count['hits_count'] = None
            for collect_task in [_ for _ in collect_tasks if _.accounts[0].platform == db_item.platform and _.accounts[0].platform_id == db_item.platform_id]:
                hits_count['hits_count'] = hits_count['hits_count'] if 'hits_count' in hits_count and hits_count['hits_count'] else 0
                hits_count['hits_count'] += collect_task.hits_count
            hits_count['hits_count'] = -2 if hits_count['hits_count'] != 0 and not hits_count['hits_count'] and not result['is_loading'] else hits_count['hits_count']
        else:
            hits_count['title'] = db_item.term
            for platform in monitor.platforms:
                hits_count[platform] = None if monitor.status <= MonitorStatus.sampling else -2
                collect_task_filtered = [ _ for _ in collect_tasks if _.search_terms[0].term == db_item.term and _.platform == platform]
                if len(collect_task_filtered):
                    hits_count[platform] = 0
                    for collect_task in collect_task_filtered:
                        hits_count[platform] += collect_task.hits_count
            # for collect_task in [ _ for _ in collect_tasks if _.search_terms[0].term == db_item.term]:
            #     hits_count[collect_task.platform] = hits_count[collect_task.platform] if collect_task.platform in hits_count else 0
            #     hits_count[collect_task.platform] += collect_task.hits_count
                hits_count[platform] = -2 if hits_count[platform] != 0 and not hits_count[platform] and not result['is_loading'] else hits_count[platform]

        result['data'].append(hits_count)

    
    is_all_loaded = all([all([(value is not None) and (type(value) != int or (value or value == 0)) for value in _.values()]) for _ in result['data']])
    result['is_loading'] = False if is_all_loaded else result['is_loading']

    result['data'] = sorted(result['data'], key=getTottal)
    return JSONResponse(content=jsonable_encoder(result), status_code=200)
    

@app.post("/search_account", response_description="Search accounts by string across all platforms")
async def search_account(request: Request, search_accounts: RequestAccountsSearch, current_email: str = Depends(get_current_user_email)):
    await mongo([Account], request)
    accounts: List[Account] = []
    
    methods = []
    for platform in collector_classes:
        # if platform in [Platform.vkontakte]: continue
        data_source = collector_classes[platform]()
        methods.append(data_source.get_accounts)

    accounts_from_platforms = await gather(*[method(search_accounts.substring) for method in methods])

    responce = []
    for account in chain.from_iterable([accounts_from_platform[:5] for accounts_from_platform in accounts_from_platforms]):
        responce.append({
            'label': account.title,
            'icon': account.platform,
            'platform_id': account.platform_id,
            'platform': account.platform,
            'url': account.url,
        })
    return JSONResponse(content=jsonable_encoder(responce), status_code=200)


nltk.download('stopwords')
stop_words = set(stopwords.words('russian'))
e_stop_words = set(stopwords.words('english'))
stop_words.update(e_stop_words)
# for stop_word in low_resource_stopwords['web'] + low_resource_stopwords['ka'] + low_resource_stopwords['hy']:
for stop_word in list(chain.from_iterable(low_resource_stopwords.values())):
    stop_words.add(stop_word.lower())


@app.post("/recommendations", response_description="Get monitor")
async def recommendations(request: Request, monitor_id: RequestId, current_email: str = Depends(get_current_user_email)):
    # return {
    #         'is_loading': False, 'recommendations': [
    #             {'word': 'testword1', 'score': .5},
    #             {'word': 'testword2 AND testword3', 'score': .3},
    #             {'word': 'testword4', 'score': .23},
    #         ]
    #     }
    await mongo([Monitor, Post], request)
    """
    :param: monitor_ids: Are ids taken from database. monitor_ids[0] is the id we want to search words for.
    :return: Returns top 10 frequent words.
    """
    # import time
    # start = time.time() 
    await mongo([Monitor, Post, CollectTask], request)

    monitor = await Monitor.get(monitor_id.id)
    # print(111, time.time() - start)

    monitor_posts = await Post.find({ 'monitor_ids': {'$in': [UUID(monitor_id.id)] }}).aggregate([{ '$sample': { 'size': 1500 } } ]).to_list()
    docs = [' '.join([post['text'] + ' ' + post['title'] for post in monitor_posts]).lower()]
    # print('[Recommendation] monitor_posts len', len(monitor_posts))

    # print(222, time.time() - start)
    
    if monitor.status < MonitorStatus.sampling and len(monitor_posts) < 200 :
        return {
            'is_loading': True
        }
    if len(monitor_posts) < 10:
        return {
            'is_loading': False,
            'recommendations': []
        }
    sample_size = 500 if len(monitor_posts) < 500 else len(monitor_posts)
    monitors = await Monitor.find({ 'monitor_ids': {'$nin': [UUID(monitor_id.id)] }}).aggregate([{ '$sample': { 'size': 3 } } ]).to_list()
    # print(333, time.time() - start)

    for other_monitor in monitors:
        other_posts = await Post.find({ 'monitor_ids': {'$in': [other_monitor['_id']] }}).limit(sample_size).to_list()
        # print('[Recommendation] other posts len', len(other_posts))
        if len(other_posts) == 0: continue
        # print(time.time() - start)
        docs.append(' '.join([post.text + ' ' + post.title for post in other_posts]).lower())
    
    keywords_already_in_monitor = await get_keywords_in_monitor(monitor_id.id, True)
    
    for stop_word in keywords_already_in_monitor:
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
    words = [{ 'word': word[1].replace(' ', ' AND '), 'score': word[0]} for word in words if word[0] > 0.1 ]
    # print(555)
    
    return JSONResponse(content=jsonable_encoder({'is_loading': False, 'recommendations': words}), status_code=200)

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
    host = 'https://un.ibex-app.com/' if env == 'dev' else request.headers['referer'].rstrip('login')
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
