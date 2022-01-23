from typing import Optional
from beanie import Document, init_beanie
from typing import List
from pydantic import Field, BaseModel, validator
from uuid import UUID, uuid4
from datetime import datetime

from enum import Enum

class TagType(str, Enum):
    topic = 'topic'
    person = 'person' 
    organization = 'organization'
    object = 'object'
    location = 'location'

class Tag(Document):
    id: UUID = Field(default_factory=uuid4, alias='_id')
    type: TagType
    title: str
    alias: Optional[List[str]]
    img_url: Optional[str]
    location: Optional[object]
    related_tags:Optional[List[UUID]]
    meta_data: Optional[object]

    class Config:  
        use_enum_values = True
        validate_assignment = True
        arbitrary_types_allowed = True

    class Collection:
        name = "tags"

class Platform(str, Enum):
    facebook = "facebook"
    youtube = "youtube"
    twitter = "twitter"
    geotv = "geotv"
    tiktok = "tiktok"

class Labels(BaseModel):
    topics: Optional[List[UUID]]
    persons: Optional[List[UUID]]
    organizations: Optional[List[UUID]]
    locations: Optional[List[UUID]]


class Scores(BaseModel):
    likes: Optional[int]
    views: Optional[int]
    engagement: Optional[int]
    shares: Optional[int]
    sad: Optional[int]
    wow: Optional[int]
    cry: Optional[int]
    love: Optional[int]


class Transcript(BaseModel):
    time: datetime
    text: str


class DataSource(Document):
    """
        These are pages/accounts that have already been identified as important. Data collection either proceeds via sources or keywords
        [indexed by name, platform]
        
        id - uuid for data source
        platform_id - the identifier provided by social media platform, page id for facebook, channel id for youtube and profile id for twitter
        title - the title of the data source, page/Chanel/profile name
        tags - tags for the data source, might be merged with already defined categories tag
        platform - facebook | twitter | youtube ...
        url - url for the data source
        img_url - profile image for data source
        
    """
    id: Optional[UUID] = Field(default_factory=uuid4, alias='_id')
    title: str
    platform: Platform
    platform_id: str
    program_title: Optional[str]
    url: str
    img: Optional[str]
    tags: List[str] = []
    broadcasting_start_time: Optional[datetime]
    broadcasting_end_time: Optional[datetime]

    class Config:  
        use_enum_values = True
        validate_assignment = True

    # @validator('name')
    # def set_name(cls, name):
    #     return name or 'foo'

    class Collection:
        name = "data_sources"


class Post(Document):
    id: UUID = Field(default_factory=uuid4, alias='_id')
    title: Optional[str]
    text: str
    created_at: datetime
    platform: Platform
    platform_id: str
    # author_platform_id: Optional[str]
    data_source_id: UUID
    hate_speech: Optional[float]
    sentiment: Optional[float]
    has_video: Optional[bool]
    video_url: Optional[str]
    api_dump: dict

    labels: Optional[Labels]
    scores: Optional[Scores]
    transcripts: Optional[List[Transcript]]

    class Config:
        use_enum_values = True
        validate_assignment = True

    class Collection:
        name = 'posts'


class PostRequestParams(BaseModel):
    time_interval_from: datetime
    time_interval_to: datetime
    has_video:Optional[bool]
    platforms: List[Platform] = []
    post_contains: Optional[str]
    data_sources: List[UUID] = []
    topics: List[UUID] = []
    persons: List[UUID] = []
    locations: List[UUID] = []

    sort_by: Optional[str]

    count: int = 100 
    start_index: int = 0


class PostRequestParamsAggregated(BaseModel):
    post_request_params: PostRequestParams
    axisX: Optional[str]
    axisY: Optional[str]
    axisZ: Optional[str]
    days: Optional[int]