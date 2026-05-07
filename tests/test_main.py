from argparse import Namespace

import pytest

from main import validate_args


def test_validate_args_rejects_non_positive_top_n():
    with pytest.raises(ValueError, match="1개 이상"):
        validate_args(Namespace(top_n=0))
