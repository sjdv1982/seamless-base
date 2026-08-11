from seamless import BoundStateError, Cell


class Backend:
    path = "root"
    path_python = "root"
    celltype = "mixed"
    target_celltype = "mixed"
    validator = None
    validator_language = None
    input_ref = None

    def derive(self, **updates):
        result = type(self)()
        result.path = updates.get("path", self.path)
        result.path_python = result.path
        return result

    def build(self, input_ref):
        raise AssertionError("not part of this contract test")


def test_bound_cell_uses_backend_path_and_api_names():
    cell = Cell._from_backend(Backend())
    assert not hasattr(Cell, "pins")
    assert isinstance(BoundStateError("x"), AttributeError)
    assert cell.path_python == "root"
    assert cell["field"].path_python == "root.field"

    try:
        cell.input_ref = "checksum"
    except BoundStateError:
        pass
    else:
        raise AssertionError("bound input_ref mutation must be rejected")
