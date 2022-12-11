from typing import Optional
from typing import List
from pydantic import BaseModel
from uuid import UUID
from datetime import datetime
from ibex_models import Platform, Annotation


class RequestPostsFilters(BaseModel):
    """Filters to apply when requesting posts.

    Parameters:
        time_interval_from (Optional[datetime]): Start time for the time interval to filter posts by.
        time_interval_to (Optional[datetime]): End time for the time interval to filter posts by.
        has_video (Optional[bool]): Filter posts by whether or not they contain a video.
        platform (List[Platform]): List of platforms to filter posts by.
        post_contains (Optional[str]): Filter posts by the text they contain.
        author_platform_id (List[str]): List of platform IDs of authors to filter posts by.
        topics (List[str]): List of topics to filter posts by.
        persons (List[str]): List of persons to filter posts by.
        locations (List[str]): List of locations to filter posts by.
        monitor_id (Optional[str]): ID of the monitor to filter posts by.

        account_ids (List[str]): List of account IDs to filter posts by.
        search_term_ids (List[str]): List of search term IDs to filter posts by.

        accounts (List[str]): List of accounts to filter posts by.
        search_terms (List[str]): List of search terms to filter posts by.

        shuffle (Optional[bool]): Whether or not to shuffle the returned posts.
        sort_by (Optional[str]): Field to sort the returned posts by.
        count (int): Number of posts to return (default is 100).
        start_index (int): Index of the first post to return (default is 0).
    """

    time_interval_from: Optional[datetime]
    time_interval_to: Optional[datetime]
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
    """Filters to apply when requesting aggregated posts.

    Parameters:
        post_request_params (RequestPostsFilters): Filters to apply to the posts.
        axisX (Optional[str]): Field to use as the X-axis in the aggregation.
        axisY (Optional[str]): Field to use as the Y-axis in the aggregation.
        axisZ (Optional[str]): Field to use as the Z-axis in the aggregation.
        days (Optional[int]): Number of days to group the aggregation by.
    """
    post_request_params: RequestPostsFilters
    axisX: Optional[str]
    axisY: Optional[str]
    axisZ: Optional[str]
    days: Optional[int]


class RequestAnnotations(BaseModel):
    """Parameters for requesting annotations.

    Parameters:
        text_id (Optional[UUID]): ID of the text to retrieve annotations for.
        annotations (Optional[List[Annotation]]): List of annotations to retrieve.
    """
    text_id: Optional[UUID]
    annotations: Optional[List[Annotation]]


class RequestId(BaseModel):
    """Parameters for requesting an MongoDB object by ID.

    Parameters:
        id (str): UUID for the object.
    """
    id: str

class RequestTag(BaseModel):
    """Parameters for requesting an MongoDB Object by Tag.

    Parameters:
        tag (str): Tag to use to request an object.
    """
    tag: str

class RequestAccountsSearch(BaseModel):
    """Parameters for searching accounts.

    Parameters:
        substring (str): Substring to search for in the accounts.
        platforms (List[Platform]): List of platforms to search the accounts from.
    """
    substring: str
    platforms: List[Platform]


class RequestAccount(BaseModel):
    """Parameters for creating a Account.

    Parameters:
        id (Optional[UUID]): ID of the account to request.
        title (str): Title of the account.
        platform (Platform): Platform the account belongs to.
        platform_id (str): ID of the account on the platform.
        url (str): URL of the account on the platform.
    """
    id: Optional[UUID]
    title: str
    platform: Platform
    platform_id: str
    url: str


class RequestMonitor(BaseModel):
    """Parameters for creating a Monitor.

    Parameters:
        title (str): Title of the Monitor.
        descr (str): Description of the Monitor.
        date_from (datetime): Start date for the Monitor.
        date_to (Optional[datetime]): End date for the Monitor.
        search_terms (Optional[List[str]]): List of search terms to use in the Monitor.
        accounts (Optional[List[RequestAccount]]): List of accounts to use in the Monitor.
        platforms (Optional[List[Platform]]): List of platforms to use in the Monitor.
        languages (Optional[List[str]]): List of languages to use in the Monitor.
    """
    title: str
    descr: str
    date_from: datetime
    date_to: Optional[datetime]
    search_terms: Optional[List[str]]
    accounts: Optional[List[RequestAccount]]
    platforms: Optional[List[Platform]]
    languages: Optional[List[str]]


class RequestSearchTerm(BaseModel):
    """Parameters for creating a SearchTerm.

    Parameters:
        id (Optional[UUID]): ID of the SearchTerm.
        term (str): Search term string.
    """
    id: Optional[UUID]
    term: str


class RequestMonitorEdit(BaseModel):
    """Parameters for editing a Monitor.

    Parameters:
        id (UUID): ID of the Monitor to edit.
        date_from (Optional[datetime]): Start date for the Monitor.
        date_to (Optional[datetime]): End date for the Monitor.
        title (Optional[str]): Title of the Monitor.
        descr (Optional[str]): Description of the Monitor.
        search_terms (Optional[List[RequestSearchTerm]]): List of search terms to use in the Monitor.
        accounts (Optional[List[RequestAccount]]): List of accounts to use in the Monitor.
        platforms (Optional[List[Platform]]): List of platforms to use in the Monitor.
        languages (Optional[List[str]]): List of languages to use in the Monitor.
        resample (Optional[bool]): Whether or not to resample the Monitor.
    """
    id: UUID
    date_from: Optional[datetime]
    date_to: Optional[datetime]
    title: Optional[str]
    desr: Optional[str]
    search_terms: Optional[List[RequestSearchTerm]]
    accounts: Optional[List[RequestAccount]]
    platforms: Optional[List[Platform]]
    languages: Optional[List[str]]
    resample: Optional[bool]


class RequestAddTagToPost(BaseModel):
    """Parameters for adding tags to a Post.

    Parameters:
        id (UUID): ID of the Post to add tags to.
        tags (List[str]): List of tags to add to the Post.
    """
    id: UUID
    tags: List[str]