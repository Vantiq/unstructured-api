from typing import Iterable, Iterator, Optional

from unstructured.chunking.base import BoundaryPredicate, is_on_next_page, PreChunker
# noinspection PyProtectedMember
from unstructured.chunking.title import _ByTitleChunkingOptions
from unstructured.documents.elements import Element
from functools import cached_property


class _ByPageChunkingOptions(_ByTitleChunkingOptions):
    @cached_property
    def boundary_predicates(self) -> tuple[BoundaryPredicate, ...]:
        """The semantic-boundary detectors to be applied to break pre-chunks.

        For the `by_page` strategy these are page boundaries.
        """

        def iter_boundary_predicates() -> Iterator[BoundaryPredicate]:
            yield is_on_next_page()

        return tuple(iter_boundary_predicates())


def chunk_by_page(
    elements: Iterable[Element],
    *,
    combine_text_under_n_chars: Optional[int] = None,
    include_orig_elements: Optional[bool] = None,
    max_characters: Optional[int] = None,
    new_after_n_chars: Optional[int] = None,
    overlap: Optional[int] = None,
    overlap_all: Optional[bool] = None,
) -> list[Element]:
    opts = _ByPageChunkingOptions.new(
        combine_text_under_n_chars=combine_text_under_n_chars,
        include_orig_elements=include_orig_elements,
        max_characters=max_characters,
        new_after_n_chars=new_after_n_chars,
        overlap=overlap,
        overlap_all=overlap_all,
    )
    pre_chunks = PreChunker.iter_pre_chunks(elements, opts)
    return [chunk for pre_chunk in pre_chunks for chunk in pre_chunk.iter_chunks()]

