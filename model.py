from typing import Optional
from beanie import Document, init_beanie, odm
from typing import List
from pydantic import Field, BaseModel, validator
from uuid import UUID, uuid4
from datetime import datetime
from ibex_models import Platform, Annotation

class PostRequestParams(BaseModel):
    time_interval_from: datetime
    time_interval_to: datetime
    has_video:Optional[bool]
    platform: List[Platform] = []
    post_contains: Optional[str]
    accounts: List[str] = []
    author_platform_id: List[str] = []
    topics: List[str] = []
    persons: List[str] = []
    locations: List[str] = []
    monitor_id: Optional[str]

    sort_by: Optional[str]

    count: int = 100 
    start_index: int = 0


class PostRequestParamsAggregated(BaseModel):
    post_request_params: PostRequestParams
    axisX: Optional[str]
    axisY: Optional[str]
    axisZ: Optional[str]
    days: Optional[int]

class RequestAnnotations(BaseModel):
    text_id: UUID
    user_mail: str
    annotations: Optional[List[Annotation]]

class IdRequestParams(BaseModel):
    id: str

class TagRequestParams(BaseModel):
    tag: str

class AccountReques(BaseModel):
    title: str
    platform: Platform
    platform_id: str

class PostMonitor(BaseModel):
    title: str
    descr: str
    date_from: datetime
    date_to: Optional[datetime]
    search_terms: List[str]
    accounts: List[AccountReques]
    platforms: Optional[List[Platform]]