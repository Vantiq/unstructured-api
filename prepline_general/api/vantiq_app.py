import io
from typing import List, Optional

import requests
from fastapi import APIRouter, UploadFile
from starlette.requests import Request
from unstructured.file_utils.filetype import detect_filetype, FILETYPE_TO_MIMETYPE

from .app import app
from .general import general_partition
from .models.form_params import GeneralFormParams

vantiq_router = APIRouter()


class PartitionUrls(GeneralFormParams):
    urls: List[str]
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


class NamedIO(io.BytesIO):
    name: str


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
    files: List[UploadFile] = []
    for url in to_partition.urls:
        # Fetch the content from the URL and treat as named ByteIO object
        response = requests.get(url)
        file = NamedIO(response.content)
        file.name = url

        # Determine the file type from the content/name
        encoding = response.headers.get("Content-Encoding", "utf-8")
        filetype = detect_filetype(file=file, encoding=encoding)

        # Construct an UploadFile object with the file and its metadata, so we can use the general_partition function
        headers = request.headers.mutablecopy()
        headers["Content-Type"] = FILETYPE_TO_MIMETYPE[filetype]
        upload = UploadFile(file= file, filename=url, headers=headers)
        files.append(upload)
    return general_partition(request, files, to_partition)


app.include_router(vantiq_router)
vantiq_app = app
