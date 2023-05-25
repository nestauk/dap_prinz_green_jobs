"""
Generically useful functions
"""

from itertools import islice


def list_chunks(orig_list: list, chunk_size: int = 100):
    """Chunks list into batches of a specified chunk_size."""
    for i in range(0, len(orig_list), n_chunks):
        yield orig_list[i : i + n_chunks]


def dict_chunks(data_dict: dict, chunk_size: int = 100):
    """Chunks data dictionary into batches of a specified chunk_size.

    Args:
        data_dict: dictionary of job adverts where key is job id
            and value is a list of skills
        chunk_size (int, optional): chunk size. Defaults to 100.

    Yields:
        _type_: job advert chunks
    """
    it = iter(data_dict)
    for i in range(0, len(data_dict), chunk_size):
        yield {k: data_dict[k] for k in islice(it, chunk_size)}
