from random import shuffle
from typing import Optional
from beanie import Document, init_beanie, odm
from typing import List
from pydantic import Field, BaseModel, validator
from uuid import UUID, uuid4
from datetime import datetime
from ibex_models import Platform, Annotation

class RequestPostsFilters(BaseModel):
    time_interval_from: Optional[str]
    time_interval_to: Optional[str]
    has_video:Optional[bool]
    platform: List[Platform] = []
    post_contains: Optional[str]
    author_platform_id: List[str] = []
    topics: List[str] = []
    persons: List[str] = []
    locations: List[str] = []
    monitor_id: Optional[str]
    
    account_ids: List[str] = []
    search_term_ids: List[str] = []

    accounts:List[str] = []
    search_terms:List[str] = []

    shuffle: Optional[bool]

    sort_by: Optional[str]

    count: int = 100 
    start_index: int = 0


class RequestPostsFiltersAggregated(BaseModel):
    post_request_params: RequestPostsFilters
    axisX: Optional[str]
    axisY: Optional[str]
    axisZ: Optional[str]
    days: Optional[int]

class RequestAnnotations(BaseModel):
    text_id: Optional[UUID]
    annotations: Optional[List[Annotation]]

class RequestId(BaseModel):
    id: str

class RequestTag(BaseModel):
    tag: str

class RequestAccountsSearch(BaseModel):
    substring: str
    platforms: List[Platform]

class RequestAccount(BaseModel):
    id: Optional[UUID]
    title: str
    platform: Platform
    platform_id: str

class RequestMonitor(BaseModel):
    title: str
    descr: str
    date_from: datetime
    date_to: Optional[datetime]
    search_terms: Optional[List[str]]
    accounts: Optional[List[RequestAccount]]
    platforms: Optional[List[Platform]]
    languages: Optional[List[str]]

class RequestSearchTerm(BaseModel):
    id: Optional[UUID]
    term: str


class RequestMonitorEdit(BaseModel):
    id: UUID
    date_from: Optional[datetime]
    date_to: Optional[datetime]
    search_terms: Optional[List[RequestSearchTerm]]
    accounts: Optional[List[RequestAccount]]
    platforms: Optional[List[Platform]]
    languages: Optional[List[str]]
    resample: Optional[bool]

class RequestAddTagToPost(BaseModel):
    id: UUID
    tags: List[str]