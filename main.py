from turtle import st
from beanie import init_beanie
import motor

from bson.binary import Binary

from datetime import datetime
from uuid import UUID
from typing import List
from ibex_models import PostRequestParams, Post, RequestAnnotations, PostRequestParamsAggregated, Annotations, TextForAnnotation

from bson import json_util, ObjectId
from bson.json_util import dumps, loads
from pydantic import Field, BaseModel, validator

import json 

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# @staticmethod
def generate_search_criteria(post_request_params: PostRequestParams):
    search_criteria = {
        'created_at': { '$gte': post_request_params.time_interval_from, '$lte': post_request_params.time_interval_to},
    }
    
    if type(post_request_params.has_video) == bool:
        search_criteria['has_video'] = { '$eq': post_request_params.has_video }

    if bool(post_request_params.post_contains):
        search_criteria['text'] = { '$regex': post_request_params.post_contains }

    if len(post_request_params.platforms) > 0:
        search_criteria['platform'] = { '$in': post_request_params.platforms }

    if len(post_request_params.data_sources) > 0:
        search_criteria['data_source_id'] = { '$in': [Binary(i.bytes, 3) for i in post_request_params.data_sources] }

    if len(post_request_params.topics) > 0:
        search_criteria['labels.topics'] = { '$in': [Binary(i.bytes, 3) for i in post_request_params.topics] }
    
    if len(post_request_params.persons) > 0:
        search_criteria['labels.persons'] = { '$in': [Binary(i.bytes, 3) for i in post_request_params.persons] }

    if len(post_request_params.locations) > 0:
        search_criteria['labels.locations'] = { '$in': [Binary(i.bytes, 3) for i in post_request_params.locations] }

    return search_criteria

async def mongo(classes):
    mongodb_connection_string = "mongodb+srv://root:Dn9B6czCKU6qFCj@cluster0.iejvr.mongodb.net/ibex?retryWrites=true&w=majority"
    client = motor.motor_asyncio.AsyncIOMotorClient(mongodb_connection_string)
    await init_beanie(database=client.ibex, document_models=classes)

def json_responce(result):
    json_result = json.loads(json_util.dumps(result))
    return JSONResponse(content=jsonable_encoder(json_result), status_code=200)


@app.post("/posts", response_description="Get list of posts", response_model=List[Post])
async def posts(post_request_params: PostRequestParams) -> List[Post]:
    await mongo([Post])
    
    search_criteria = generate_search_criteria(post_request_params)

    result = await Post.find(search_criteria)\
        .aggregate([
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
                    'from': "data_sources",
                    'localField': f"data_source_id",
                    'foreignField': "_id",
                    'as': "data_source"
                }
            },
        ])\
        .to_list()

    for result_ in result:
        result_['api_dump'] = ''
        # for result_key in result_.keys():
        #     if type(result_[result_key]) == float:
        #         result_[result_key] = 'NN'
        
    return json_responce(result)


@app.post("/posts_aggregated", response_description="Get aggregated data for posts")#, response_model=List[Post])
async def posts_aggregated(post_request_params_aggregated: PostRequestParamsAggregated) -> List[Post]:

    await mongo([Post])
    
    search_criteria = generate_search_criteria(post_request_params_aggregated.post_request_params)
    
    aggregation = {}
    if post_request_params_aggregated.days is None:
        aggregation = f"$labels.{post_request_params_aggregated.axisX}"
    else:
        aggregation = {
            "label": f"$labels.{post_request_params_aggregated.axisX}",
            "year": { "$year": "$created_at" },
        }
        if post_request_params_aggregated.days == 30:
            aggregation["month"] = { "$month": "$created_at" }
        if post_request_params_aggregated.days == 7:
            aggregation["week"] = { "$weekOfYear": "$created_at" }
        if post_request_params_aggregated.days == 1:
            aggregation["day"] = { "$dayOfYear": "$created_at" }
        
        
    result = await Post.find(search_criteria)\
        .aggregate([
            {'$unwind':f"$labels.{post_request_params_aggregated.axisX}" },
            {'$group':{
                '_id': aggregation , 
                'count': {'$sum':1} 
                } 
            },
            {
                '$lookup': {
                    'from': "tags",
                    'localField': f"_id{'.label' if post_request_params_aggregated.days is not None else ''}",
                    'foreignField': "_id",
                    'as': f"{post_request_params_aggregated.axisX}"
                }
            },
            {'$unwind': f"${post_request_params_aggregated.axisX}" },
        ])\
        .to_list()

    return json_responce(result)

class PostRequestParamsSinge(BaseModel):
    id: str

@app.post("/post", response_description="Get post details")
async def post(postRequestParamsSinge: PostRequestParamsSinge) -> Post:
    await mongo([Post])
    id_ = loads(f'{{"$oid":"{postRequestParamsSinge.id}"}}')

    post = await Post.find_one(Post.id == id_)
    post.api_dump = {}
    return json_responce(post)


@app.post("/save_and_next", response_description="Save the annotations for the text and return new text for annotation", response_model=TextForAnnotation)
async def save_and_next(request_annotations: RequestAnnotations) -> TextForAnnotation:
    await mongo([Annotations, TextForAnnotation])
    
    annotations = Annotations(text_id = request_annotations.text_id, user_mail = request_annotations.user_mail, annotations = request_annotations.annotations)

    await annotations.insert()

    already_annotated = await Annotations.aggregate([
        {"$match": { "user_mail": { "$eq": 'djanezashvili@gmail.com' }}}
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

    text_for_annotation = TextForAnnotation(id=text_for_annotation[0]["_id"], post_id = text_for_annotation[0]["text"]["post_id"], words=text_for_annotation[0]["text"]["words"])
    return text_for_annotation


# curl -X 'GET' \
#   'http://127.0.0.1:8000/posts_aggregated' \
#   -H 'accept: application/json' \
#   -H 'Content-Type: application/json' \
#   -d '{
#   "post_request_params": {
#     "time_interval_from": "2021-01-16T17:23:05.925Z",
#     "time_interval_to": "2021-07-16T17:23:05.925Z",
#   },
#   "axisX": "topics",
#   "days": 30
# }'
