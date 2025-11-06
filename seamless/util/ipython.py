"""Tools for interfacing with IPython"""

import sys

IPythonInputSplitter = None
MyInProcessKernelManager = None  # type: ignore
TransformerManager = None
_IPYTHON_MAJOR_VERSION = None


def _imp():
    global IPythonInputSplitter, MyInProcessKernelManager
    from IPython.core.inputsplitter import (  # pylint: disable=redefined-outer-name # type: ignore
        IPythonInputSplitter,
    )
    from ipykernel.inprocess.ipkernel import InProcessKernel
    from ipykernel.inprocess.manager import InProcessKernelManager
    from ipykernel.zmqshell import ZMQInteractiveShell

    class MyInProcessKernel(InProcessKernel):
        """Replacement for IPython's InProcessKernel"""

        # get rid of singleton shell instance!
        class dummy:
            """Dummy interactive shell class that is not a singleton"""

            def instance(self, *args, **kwargs):
                """Get instance, but not singleton"""
                shell = ZMQInteractiveShell(*args, **kwargs)
                return shell

        shell_class = dummy()

    class MyInProcessKernelManager(  # pylint: disable=unused-variable
        InProcessKernelManager
    ):  # pylint: disable=redefined-outer-name
        """Replacement for IPython's InProcessKernelManager"""

        def start_kernel(self, namespace):  # pylint: disable=arguments-differ
            self.kernel = MyInProcessKernel(
                parent=self, session=self.session, user_ns=namespace
            )


def _imp_transformer_manager():
    """Lazy import for the IPython 9+ transformer manager."""
    global TransformerManager
    if TransformerManager is not None:
        return
    try:
        from IPython.core.inputtransformer2 import (
            TransformerManager as _TransformerManager,
        )
    except ImportError as exc:  # pragma: no cover - defensive, depends on IPython
        raise RuntimeError("IPython >= 9 required for ipython2python_ipy9") from exc
    TransformerManager = _TransformerManager


def _get_ipython_major_version():
    """Retrieve and cache the IPython major version number."""
    global _IPYTHON_MAJOR_VERSION
    if _IPYTHON_MAJOR_VERSION is not None:
        return _IPYTHON_MAJOR_VERSION
    try:
        import IPython  # pylint: disable=import-outside-toplevel
    except ImportError as exc:  # pragma: no cover - environment dependent
        raise RuntimeError("IPython is required for ipython2python") from exc
    version_info = getattr(IPython, "version_info", None)
    if version_info is not None:
        major = version_info[0]
    else:  # pragma: no cover - very old IPython versions
        version_str = getattr(IPython, "__version__", "0")
        try:
            major = int(version_str.split(".")[0])
        except ValueError as err:
            raise RuntimeError(
                f"Unable to parse IPython version '{version_str}'"
            ) from err
    _IPYTHON_MAJOR_VERSION = major
    return major


def execute(code, namespace):
    """Executes Python code in an IPython kernel

    If the code is in IPython format (i.e. with % and %% magics),
     run it through ipython2python first
    """
    if MyInProcessKernelManager is None:
        _imp()
    kernel_manager = MyInProcessKernelManager()  # pylint: disable=not-callable
    kernel_manager.start_kernel(namespace)
    kernel = kernel_manager.kernel

    result = kernel.shell.run_cell(code, False)
    if result.error_before_exec is not None:
        print(result.error_before_exec, file=sys.stderr)
    if result.error_in_exec is not None:
        print(result.error_in_exec, file=sys.stderr)
    if not result.success:
        if kernel.shell._last_traceback:
            for tb in kernel.shell._last_traceback:
                print(tb, file=sys.stderr)
    return namespace


def ipython2python_ipy8(code):
    """Convert IPython code (including magics) to normal Python code on IPython 8."""
    if IPythonInputSplitter is None:
        _imp()
    isp = IPythonInputSplitter()  # type: ignore # pylint: disable=not-callable
    newcode = ""
    for line in code.splitlines():
        if isp.push_accepts_more():
            isp.push(line.strip("\n"))
            continue
        cell = isp.source_reset()
        if cell.startswith("get_ipython().run_"):
            cell = "_ = " + cell
        newcode += cell + "\n"
        isp.push(line.strip("\n"))
    cell = isp.source_reset()
    if len(cell):
        if cell.startswith("get_ipython().run_"):
            cell = "_ = " + cell
        newcode += cell
    return newcode


def ipython2python_ipy9(code):
    """Convert IPython code (including magics) to normal Python code on IPython 9+."""
    if TransformerManager is None:
        _imp_transformer_manager()
    manager = TransformerManager()  # type: ignore # pylint: disable=not-callable
    transformed = manager.transform_cell(code)
    converted_chunks = []
    for chunk in transformed.splitlines(True):
        if chunk.startswith("get_ipython().run_"):
            chunk = "_ = " + chunk
        converted_chunks.append(chunk)
    return "".join(converted_chunks)


def ipython2python(code):
    """Convert IPython code to normal Python, selecting an implementation per version."""
    major = _get_ipython_major_version()
    if major >= 9:
        return ipython2python_ipy9(code)
    return ipython2python_ipy8(code)
