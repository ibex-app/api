from beanie import init_beanie
import motor

from bson.binary import Binary

from datetime import datetime
from uuid import UUID
from typing import List
from model import PostRequestParams, Post, Tag, Platform, DataSource, PostRequestParamsAggregated


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
        ])\
        .to_list()
    return JSONResponse(content=jsonable_encoder(result), status_code=200)


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

    return JSONResponse(content=jsonable_encoder(result), status_code=200)


@app.get("/post/{id}", response_description="Get post details", response_model=List[Post])
async def posts(id: UUID) -> Post:
    await mongo([Post])
    
    post = await Post.find_one(Post.id == Binary(id.bytes, 3))
    return post



# def save_and_next_post(post_id: str, user_id: str, annotations: list[str]) -> Post:
#     db_post:Post = Post()
#     db_post.uuid = post_id

#     db_post.annotations = annotations

#     next_post:Post = Post()

#     user_id not in next_post.annotators 
#     len(next_post.annotators) < 4

#     return next_post


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