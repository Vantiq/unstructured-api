import os
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from tempfile import TemporaryDirectory, SpooledTemporaryFile
from typing import List, Optional, Union

import requests
from fastapi import APIRouter, UploadFile
from pydantic import BaseModel
from starlette.requests import Request
from unstructured.file_utils.filetype import detect_filetype, FILETYPE_TO_MIMETYPE, FileType

from .app import app
from .general import general_partition
from .models.form_params import GeneralFormParams

vantiq_router = APIRouter()


class UrlWithContext(BaseModel):
    url: str
    content_type: Optional[str] = None
    headers: Optional[dict] = None
    filename: Optional[str] = None


class PartitionUrls(GeneralFormParams):
    urls: List[Union[str, UrlWithContext]]
    xml_keep_tags: bool = False
    languages: Optional[List[str]] = []
    ocr_languages: Optional[List[str]] = []
    skip_infer_table_types: Optional[List[str]] = []
    gz_uncompressed_content_type: Optional[str] = None
    output_format: str = "application/json"
    coordinates: bool = False
    encoding: str = "utf-8"
    hi_res_model_name: Optional[str] = None
    include_page_breaks: bool = False
    pdf_infer_table_structure: bool = False
    strategy: str = "auto"
    extract_image_block_types: Optional[List[str]] = None
    unique_element_ids: bool = False
    # -- chunking options --
    chunking_strategy: Optional[str] = None
    combine_under_n_chars: Optional[int] = None
    max_characters: int = 500
    multipage_sections: bool = True
    new_after_n_chars: Optional[int] = None
    overlap: int = 0
    overlap_all: bool = False


@vantiq_router.post(
    "/general/v0/urls",
    openapi_extra={"x-speakeasy-name-override": "partition_url"},
    tags=["urls"],
    summary="Summary",
    description="Description",
    operation_id="partition_parameters",
)
@vantiq_router.post("/general/v0.0.73/urls", include_in_schema=False)
def partition_urls(
    request: Request,
    to_partition: PartitionUrls,
):
    # Create a temp directory to store the files
    temp_root = os.environ.get("UNSTRUCTURED_DOWNLOAD_DIR", None)
    with TemporaryDirectory(dir=temp_root) as temp_dir:
        # Curry processing function with the request and directory
        download_func = partial(download_for_processing, request=request, dir_name=temp_dir)
        thread_count = int(os.environ.get("UNSTRUCTURED_DOWNLOAD_THREADS", 2))

        # Download the files (possibly concurrently)
        files: List[UploadFile] = []
        if len(to_partition.urls) == 1 or thread_count == 1:
            for url in to_partition.urls:
                files.append(download_func(url))
        else:
            with ThreadPoolExecutor(max_workers=thread_count) as executor:
                for result in executor.map(download_func, to_partition.urls):
                    files.append(result)

        # Partition the files
        return general_partition(request, files, to_partition)


# noinspection PyAbstractClass
class NameOverride(SpooledTemporaryFile):
    name_override: str

    @property
    def name(self):
        return self.name_override


def download_for_processing(entry: Union[str, UrlWithContext], request: Request, dir_name: str) -> UploadFile:
    # Is this just a URL or the URL with a content type?
    if isinstance(entry, UrlWithContext):
        url = entry.url
        content_type = entry.content_type
        url_headers = entry.headers
        filename = entry.filename or url
    else:
        url = entry
        content_type = None
        url_headers = None
        filename = url

    # Fetch the content from the URL to a temp file
    tmp_file = SpooledTemporaryFile(max_size=10 * 1024 * 1024, dir=dir_name)
    response = requests.get(url, headers=url_headers, stream=True)
    for chunk in response.iter_content(chunk_size=1024*1024):
        tmp_file.write(chunk)
    tmp_file.seek(0)

    # Determine the file type from the content/name (if not provided)
    if not content_type:
        # Start by passing the filename to the detection function.
        encoding = response.headers.get("Content-Encoding", "utf-8")
        filetype = detect_filetype(file=tmp_file, file_filename=filename, encoding=encoding)
        if filetype is FileType.UNK:
            # If the file type is still unknown, try again without the explicit filename, but first we need
            # to monkey patch the temp file so that when the code asks for a name, it gets one (don't ask).
            tmp_file.__class__ = NameOverride
            tmp_file.name_override = filename
            filetype = detect_filetype(file=tmp_file, encoding=encoding)
        content_type = FILETYPE_TO_MIMETYPE[filetype]

    # Construct an UploadFile object with the file and its metadata, so we can use the general_partition function
    headers = request.headers.mutablecopy()
    headers["Content-Type"] = content_type
    # noinspection PyTypeChecker
    return UploadFile(file=tmp_file, filename=filename, headers=headers)


app.include_router(vantiq_router)
vantiq_app = app
