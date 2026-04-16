import numpy as np

from seamless import Buffer
from seamless.util.mixed.io import deserialize, serialize


def _nested_struct_array():
    meta_dtype = np.dtype([("id", "int32"), ("scale", "float64")], align=True)
    dtype = np.dtype([("meta", meta_dtype), ("value", "float64")], align=True)
    samples = np.zeros(2, dtype=dtype)
    samples["meta"]["id"] = [1, 2]
    samples["meta"]["scale"] = [10.0, 100.0]
    samples["value"] = [3.0, 4.0]
    return samples


def test_mixed_nested_struct_array_roundtrip():
    samples = _nested_struct_array()

    restored, storage = deserialize(serialize(samples))

    assert storage == "pure-binary"
    assert restored.dtype == samples.dtype
    np.testing.assert_array_equal(restored["meta"]["id"], samples["meta"]["id"])
    np.testing.assert_allclose(restored["meta"]["scale"], samples["meta"]["scale"])
    np.testing.assert_allclose(restored["value"], samples["value"])


def test_buffer_mixed_nested_struct_array_roundtrip():
    samples = _nested_struct_array()

    restored = Buffer(samples, "mixed").get_value("mixed")

    assert restored.dtype == samples.dtype
    np.testing.assert_array_equal(restored["meta"]["id"], samples["meta"]["id"])
    np.testing.assert_allclose(restored["meta"]["scale"], samples["meta"]["scale"])
    np.testing.assert_allclose(restored["value"], samples["value"])


def test_buffer_mixed_nested_struct_scalar_roundtrip():
    samples = _nested_struct_array()
    scalar = samples[0]

    restored = Buffer(scalar, "mixed").get_value("mixed")

    assert isinstance(restored, np.void)
    assert restored.dtype == scalar.dtype
    assert restored["meta"]["id"] == scalar["meta"]["id"]
    assert restored["meta"]["scale"] == scalar["meta"]["scale"]
    assert restored["value"] == scalar["value"]
