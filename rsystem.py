# !/usr/bin/env python
# -*- coding: utf-8 -*-

"""
@Time    : 2022-12-05 14:09:42
@Author  : Rey
@Contact : reyxbo@163.com
@Explain : Interpreter system methods.
"""


from typing import Any, List, Dict, Tuple, Iterable, Literal, Optional, Sequence, Callable, Union, NoReturn, Type, overload
from types import TracebackType
from inspect import signature as inspect_signature, _ParameterKind, _empty
from sys import path as sys_path, modules as sys_modules, exc_info as sys_exc_info
from os import getpid as os_getpid
from os.path import exists as os_exists, abspath as os_abspath
from psutil import (
    boot_time as psutil_boot_time,
    cpu_count as psutil_cpu_count,
    cpu_freq as psutil_cpu_freq,
    cpu_percent as psutil_cpu_percent,
    virtual_memory as psutil_virtual_memory,
    disk_partitions as psutil_disk_partitions,
    disk_usage as psutil_disk_usage,
    pids as psutil_pids,
    net_connections as psutil_net_connections,
    users as psutil_users,
    net_connections as psutil_net_connections,
    process_iter as psutil_process_iter,
    pid_exists as psutil_pid_exists,
    Process
)
from traceback import format_exc, format_stack, extract_stack
from warnings import warn as warnings_warn
from subprocess import Popen, PIPE
from argparse import ArgumentParser
from time import sleep as time_sleep
from datetime import datetime
from varname import argname


__all__ = (
    "add_env_path",
    "reset_env_path",
    "del_modules",
    "dos_command",
    "dos_command_var",
    "throw",
    "warn",
    "block",
    "catch_exc",
    "check_least_one",
    "check_most_one",
    "check_file_found",
    "check_file_exist",
    "check_response_code",
    "is_iterable",
    "is_table",
    "is_number_str",
    "get_first_notnull",
    "get_name",
    "get_stack_text",
    "get_stack_param",
    "get_arg_info",
    "get_computer_info",
    "get_network_table",
    "get_process_table",
    "search_process",
    "kill_process",
    "stop_process",
    "start_process",
    "get_idle_port"
)


# Added environment path.
_add_env_paths: List[str] = []


def add_env_path(path: str) -> List[str]:
    """
    Add environment variable path.

    Parameters
    ----------
    path : Path, can be a relative path.

    Returns
    -------
    Added environment variables list.
    """

    # Absolute path.
    abs_path = os_abspath(path)

    # Add.
    global _add_env_paths
    _add_env_paths.append(abs_path)
    sys_path.append(abs_path)

    return sys_path


def reset_env_path() -> None:
    """
    Reset environment variable path.
    """

    # Delete.
    global _add_env_paths
    for path in _add_env_paths:
        sys_path.remove(path)
    _add_env_paths = []


def del_modules(path: str) -> List[str]:
    """
    Delete record of modules import dictionary.

    Parameters
    ----------
    path : Module path, use regular match.

    Returns
    -------
    Deleted modules dictionary.
    """

    # Import.
    from .rregex import search

    # Set parameter.
    deleted_dict = {}
    module_keys = tuple(sys_modules.keys())

    # Delete.
    for key in module_keys:
        module = sys_modules.get(key)

        ## Filter non file module.
        if (
            not hasattr(module, "__file__")
            or module.__file__ is None
        ):
            continue

        ## Match.
        result = search(path, module.__file__)
        if result is None:
            continue

        ## Take out.
        deleted_dict[key] = sys_modules.pop(key)

    return deleted_dict


def dos_command(command: Union[str, Iterable[str]]) -> str:
    """
    Execute DOS command.

    Parameters
    ----------
    command : DOS command.
        - `str` : Use this command.
        - `Iterable[str]` : Join strings with space as command.
            When space in the string, automatic add quotation mark (e.g., ['echo', 'a b'] -> 'echo "a b"').

    Returns
    -------
    Command standard output.
    """

    # Execute.
    popen = Popen(command, shell=True, stdout=PIPE, stderr=PIPE)

    # Check.
    error_bytes: bytes = popen.stderr.read()
    if error_bytes != b"":
        error = error_bytes.decode("GBK")
        throw(value=error)

    # Standard output.
    output_bytes: bytes = popen.stdout.read()
    output = output_bytes.decode("GBK")

    return output


def dos_command_var(*vars: Any) -> List[Any]:
    """
    Use DOS command to input arguments to variables.
    Use DOS command `python file --help` to view help information.

    Parameters
    ----------
    vars : Variables.

    Returns
    -------
    Value of variables.

    Examples
    --------
    >>> var1 = 1
    >>> var2 = 2
    >>> var3 = 3
    >>> var1, var2, var3 = dos_command(var1, var2, var3)
    >>> print(var1, var2, var3)
    >>> # Use DOS command 'python file.py 10 --var2 20 21'
    10 [20, 21] 3
    """

    # Get parameter.
    vars_name = get_name(vars)
    vars_info = tuple(zip(vars_name, vars))

    # Set DOS command.
    usage = "input arguments to variables"
    parser = ArgumentParser(usage=usage)
    for name, value in vars_info:
        if value is None:
            var_type = str
            var_help = None
        else:
            var_type = value.__class__
            var_help = str(value.__class__)

        ## Position argument.
        parser.add_argument(
            name,
            nargs="?",
            type=var_type,
            help=var_help
        )

        ## Keyword argument.
        kw_name = "--" + name
        parser.add_argument(
            kw_name,
            nargs="*",
            type=var_type,
            help=var_help,
            metavar="value",
            dest=kw_name
        )

    # Get argument.
    namespace = parser.parse_args()
    values = []
    for name, value in vars_info:
        kw_name = "--" + name

        ## Position argument.
        dos_value = getattr(namespace, name)
        if dos_value is not None:
            values.append(dos_value)
            continue

        ## Keyword argument.
        dos_value = getattr(namespace, kw_name)
        if dos_value.__class__ == list:
            value_len = len(dos_value)
            if value_len == 0:
                dos_value = None
            elif value_len == 1:
                dos_value = dos_value[0]
            values.append(dos_value)
            continue

        values.append(value)

    return values


def throw(
    exception: Type[BaseException] = AssertionError,
    value: Optional[Any] = None,
    frame: int = 2
) -> NoReturn:
    """
    Throw exception.

    Parameters
    ----------
    exception : Exception Type.
    value : Exception value.
    frame : Number of code to upper level.
    """

    # Get parameter.

    ## Value name.
    if value is not None:
        name = get_name(value, frame)
        if name is None:
            value_name = "now"
        else:
            value_name = "parameter '%s'" % name

    ## Text.
    if exception == AssertionError:
        text = "use error"
    elif exception == ValueError:
        text = "value error"
    elif exception == TypeError:
        text = "value type error"
        if value is not None:
            value = value.__class__
    elif exception == FileNotFoundError:
        text = "file path not found"
    elif exception == FileExistsError:
        text = "file path already exists"
    elif exception == TimeoutError:
        text = "execute timeout"
        if value.__class__ == float:
            value = round(value, 3)
            if value % 1 == 0:
                value = int(value)
    elif exception == ConnectionError:
        text = "connection failed"

    ### Other.
    else:
        exception = AssertionError
        text = "use error"

    ### Join.
    if value is not None:
        text += ", %s is %s" % (value_name, repr(value))

    # Raise.
    exception = exception(text)
    raise exception


def warn(
    *infos: Any,
    exception: Type[BaseException] = UserWarning,
    stacklevel: int = 3
) -> None:
    """
    Throw warning.

    Parameters
    ----------
    infos : Warn informations.
    exception : Exception type.
    stacklevel : Warning code location, number of recursions up the code level.
    """

    # Handle parameter.
    if infos == ():
        infos = "Warning!"
    elif len(infos) == 1:
        if infos[0].__class__ == str:
            infos = infos[0]
        else:
            infos = str(infos[0])
    else:
        infos = str(infos)

    # Throw warning.
    warnings_warn(infos, exception, stacklevel)


def block() -> None:
    """
    Blocking program, can be double press interrupt to end blocking.
    """

    # Start.
    print("Start blocking.")
    while True:
        try:
            time_sleep(1)
        except KeyboardInterrupt:

            # Confirm.
            try:
                print("Double press interrupt to end blocking.")
                time_sleep(1)

            # End.
            except KeyboardInterrupt:
                print("End blocking.")
                break

            except:
                continue


def catch_exc(
    title: Optional[str] = None
) -> Tuple[str, Type[BaseException], BaseException, TracebackType]:
    """
    Catch exception information and print, must used in `except` syntax.

    Parameters
    ----------
    title : Print title.
        - `None` : Not print.
        - `str` : Print and use this title.

    Returns
    -------
    Exception data.
        - `str` : Exception report text.
        - `Type[BaseException]` : Exception type.
        - `BaseException` : Exception instance.
        - `TracebackType` : Exception traceback instance.
    """

    # Get parameter.
    exc_report = format_exc()
    exc_report = exc_report.strip()
    exc_type, exc_instance, exc_traceback = sys_exc_info()

    # Print.
    if title is not None:

        ## Import.
        from .rstdout import echo

        ## Execute.
        echo(exc_report, title=title, frame="half")

    return exc_report, exc_type, exc_instance, exc_traceback


def check_least_one(*values: Any) -> None:
    """
    Check that at least one of multiple values is not null, when check fail, then throw exception.

    Parameters
    ----------
    values : Check values.
    """

    # Check.
    for value in values:
        if value is not None:
            return

    # Throw exception.
    vars_name = get_name(values)
    if vars_name is not None:
        vars_name_de_dup = list(set(vars_name))
        vars_name_de_dup.sort(key=vars_name.index)
        vars_name_str = " " + " and ".join([f"'{var_name}'" for var_name in vars_name_de_dup])
    else:
        vars_name_str = ""
    raise TypeError(f"at least one of parameters{vars_name_str} is not None")


def check_most_one(*values: Any) -> None:
    """
    Check that at most one of multiple values is not null, when check fail, then throw exception.

    Parameters
    ----------
    values : Check values.
    """

    # Check.
    none_count = 0
    for value in values:
        if value is not None:
            none_count += 1

    # Throw exception.
    if none_count > 1:
        vars_name = get_name(values)
        if vars_name is not None:
            vars_name_de_dup = list(set(vars_name))
            vars_name_de_dup.sort(key=vars_name.index)
            vars_name_str = " " + " and ".join([f"'{var_name}'" for var_name in vars_name_de_dup])
        else:
            vars_name_str = ""
        raise TypeError(f"at most one of parameters{vars_name_str} is not None")


def check_file_found(path: str) -> None:
    """
    Check if file path found, if not, throw exception.

    Parameters
    ----------
    path : File path.
    """

    # Check.
    exist = os_exists(path)

    # Raise.
    if not exist:
        throw(FileNotFoundError, path, 3)


def check_file_exist(path: str) -> None:
    """
    Check if file path exist, if exist, throw exception.

    Parameters
    ----------
    path : File path.
    """

    # Check.
    exist = os_exists(path)

    # Raise.
    if exist:
        throw(FileExistsError, path, 3)


def check_response_code(
    code: int,
    range_: Optional[Union[int, Iterable[int]]] = None
) -> bool:
    """
    Check if the response code is in range.

    Parameters
    ----------
    code : Response code.
    range_ : Pass the code range.
        - `None`: Check if is between 200 and 299.
        - `int` : Check if is this value.
        - `Iterable` : Check if is in sequence.

    Returns
    -------
    Check result.
    """

    # Check.
    if range_ is None:
        result = code // 100 == 2
    elif range_.__class__ == int:
        result = code == range_
    elif hasattr(range_, "__contains__"):
        result = code in range_
    else:
        throw(TypeError, range_)

    # Raise.
    if not result:
        throw(value=code, frame=3)

    return result


def is_iterable(
    obj: Any,
    exclude_types: Iterable[Type] = [str, bytes]
) -> bool:
    """
    Judge whether it is iterable.

    Parameters
    ----------
    obj : Judge object.
    exclude_types : Non iterative types.

    Returns
    -------
    Judgment result.
    """

    # Exclude types.
    if obj.__class__ in exclude_types:
        return False

    # Judge.
    if hasattr(obj, "__iter__"):
        return True
    else:
        return False


def is_table(
    obj: Any,
    check_fields: bool = True
) -> bool:
    """
    Judge whether it is `List[Dict]` table format and keys and keys sort of the Dict are the same.

    Parameters
    ----------
    obj : Judge object.
    check_fields : Do you want to check the keys and keys sort of the Dict are the same.

    Returns
    -------
    Judgment result.
    """

    # Judge.
    if obj.__class__ != list:
        return False
    for element in obj:
        if element.__class__ != dict:
            return False

    ## Check fields of table.
    if check_fields:
        keys_strs = [
            ":".join([str(key) for key in element.keys()])
            for element in obj
        ]
        keys_strs_only = set(keys_strs)
        if len(keys_strs_only) != 1:
            return False

    return True


def is_number_str(
    string: str
) -> bool:
    """
    Judge whether it is number string.

    Parameters
    ----------
    string : String.

    Returns
    -------
    Judgment result.
    """

    # Judge.
    try:
        float(string)
    except (ValueError, TypeError):
        return False

    return True


def get_first_notnull(
    *values: Any,
    default: Union[None, Any, Literal["exception"]] = None,
    nulls: Tuple = (None,)) -> Any:
    """
    Get the first value that is not null.

    Parameters
    ----------
    values : Check values.
    default : When all are null, then return this is value, or throw exception.
        - `Any` : Return this is value.
        - `Literal['exception']` : Throw `exception`.

    nulls : Range of null values.

    Returns
    -------
    Return first not null value, when all are `None`, then return default value.
    """

    # Get value.
    for value in values:
        if value not in nulls:
            return value

    # Throw exception.
    if default == "exception":
        vars_name = get_name(values)
        if vars_name is not None:
            vars_name_de_dup = list(set(vars_name))
            vars_name_de_dup.sort(key=vars_name.index)
            vars_name_str = " " + " and ".join([f"'{var_name}'" for var_name in vars_name_de_dup])
        else:
            vars_name_str = ""
        raise ValueError(f"at least one of parameters{vars_name_str} is not None")

    return default


def get_name(obj: Any, frame: int = 2) -> Optional[Union[str, Tuple[str, ...]]]:
    """
    Get object name.
    Cannot get name of element in the sequence.

    Parameters
    ----------
    obj : Object.
    frame : Number of code to upper level.

    Returns
    -------
    Object name or None.
    """

    # Get name using built in method.
    if hasattr(obj, "__name__"):
        name = obj.__name__
        return name

    # Get name using module method.
    name = "obj"
    for frame_ in range(1, frame + 1):
        if name.__class__ != str:
            return
        name = argname(name, frame=frame_)
    if name.__class__ == tuple:
        for element in name:
            if element.__class__ != str:
                return

    return name


def get_stack_text(format_: Literal["plain", "full"] = "plain", limit: int = 2) -> str:
    """
    Get code stack text.

    Parameters
    ----------
    format_ : Stack text format.
        - `Literal['plain']` : Floor stack position.
        - `Literal['full']` : Full stack information.

    limit : Stack limit level.

    Returns
    -------
    Code stack text.
    """

    # Plain.
    if format_ == "plain":
        limit += 1
        stacks = format_stack(limit=limit)

        ## Check.
        if len(stacks) != limit:
            throw(value=limit)

        ## Convert.
        text = stacks[0]
        index_end = text.find(", in ")
        text = text[2:index_end]

    # Full.
    elif format_ == "full":
        stacks = format_stack()
        index_limit = len(stacks) - limit
        stacks = stacks[:index_limit]

        ## Check.
        if len(stacks) == 0:
            throw(value=limit)

        ## Convert.
        stacks = [
            stack[2:].replace("\n  ", "\n", 1)
            for stack in stacks
        ]
        text = "".join(stacks)
        text = text[:-1]

    # Raise.
    else:
        throw(ValueError, format_)

    return text


@overload
def get_stack_param(format_: Literal["floor"] = "floor", limit: int = 2) -> Dict: ...

@overload
def get_stack_param(format_: Literal["full"] = "floor", limit: int = 2) -> List[Dict]: ...

def get_stack_param(format_: Literal["floor", "full"] = "floor", limit: int = 2) -> Union[Dict, List[Dict]]:
    """
    Get code stack parameters.

    Parameters
    ----------
    format_ : Stack parameters format.
        - `Literal['floor']` : Floor stack parameters.
        - `Literal['full']` : Full stack parameters.

    limit : Stack limit level.

    Returns
    -------
    Code stack parameters.
    """

    # Get.
    stacks = extract_stack()
    index_limit = len(stacks) - limit
    stacks = stacks[:index_limit]

    # Check.
    if len(stacks) == 0:
        throw(value=limit)

    # Convert.

    ## Floor.
    if format_ == "floor":
        stack = stacks[-1]
        params = {
            "filename": stack.filename,
            "lineno": stack.lineno,
            "name": stack.name,
            "line": stack.line
        }

    ## Full.
    elif format_ == "full":
        params = [
            {
                "filename": stack.filename,
                "lineno": stack.lineno,
                "name": stack.name,
                "line": stack.line
            }
            for stack in stacks
        ]

    return params


def get_arg_info(func: Callable) -> List[
    Dict[
        Literal["name", "type", "annotation", "default"],
        Optional[str]
    ]
]:
    """
    Get function arguments information.

    Parameters
    ----------
    func : Function.

    Returns
    -------
    Arguments information.
        - `Value of key 'name'` : Argument name.
        - `Value of key 'type'` : Argument bind type.
            * `Literal['position_or_keyword']` : Is positional argument or keyword argument.
            * `Literal['var_position']` : Is variable length positional argument.
            * `Literal['var_keyword']` : Is variable length keyword argument.
            * `Literal['only_position']` : Is positional only argument.
            * `Literal['only_keyword']` : Is keyword only argument.
        - `Value of key 'annotation'` : Argument annotation.
        - `Value of key 'default'` : Argument default value.
    """

    # Get signature.
    signature = inspect_signature(func)

    # Get information.
    info = [
        {
            "name": name,
            "type": (
                "position_or_keyword"
                if parameter.kind == _ParameterKind.POSITIONAL_OR_KEYWORD
                else "var_position"
                if parameter.kind == _ParameterKind.VAR_POSITIONAL
                else "var_keyword"
                if parameter.kind == _ParameterKind.VAR_KEYWORD
                else "only_position"
                if parameter.kind == _ParameterKind.POSITIONAL_ONLY
                else "only_keyword"
                if parameter.kind == _ParameterKind.KEYWORD_ONLY
                else None
            ),
            "annotation": parameter.annotation,
            "default": parameter.default
        }
        for name, parameter in signature.parameters.items()
    ]

    # Replace empty.
    for row in info:
        for key, value in row.items():
            if value == _empty:
                row[key] = None

    return info


def get_computer_info() -> Dict[
    Literal[
        "boot_time",
        "cpu_count",
        "cpu_frequency",
        "cpu_percent",
        "memory_total",
        "memory_percent",
        "disk_total",
        "disk_percent",
        "process_count",
        "network_count",
        "users"
    ],
    Any
]:
    """
    Get computer information.

    Returns
    -------
    Computer information dictionary.
        - `Key 'boot_time'` : Computer boot time.
        - `Key 'cpu_count'` : Computer logical CPU count.
        - `Key 'cpu_frequency'` : Computer current CPU frequency.
        - `Key 'cpu_percent'` : Computer CPU usage percent.
        - `Key 'memory_total'` : Computer memory total gigabyte.
        - `Key 'memory_percent'` : Computer memory usage percent.
        - `Key 'disk_total'` : Computer disk total gigabyte.
        - `Key 'disk_percent'` : Computer disk usage percent.
        - `Key 'process_count'` : Computer process count.
        - `Key 'network_count'` : Computer network count.
        - `Key 'login_users'` : Computer login users information.
    """

    # Set parameter.
    info = {}

    # Get.

    ## Boot time.
    boot_time = psutil_boot_time()
    info["boot_time"] = datetime.fromtimestamp(
        boot_time
    ).strftime(
        "%Y-%m-%d %H:%M:%S"
    )

    ## CPU.
    info["cpu_count"] = psutil_cpu_count()
    info["cpu_frequency"] = int(psutil_cpu_freq().current)
    info["cpu_percent"] = round(psutil_cpu_percent(), 1)

    ## Memory.
    memory_info = psutil_virtual_memory()
    info["memory_total"] = round(memory_info.total / 1024 / 1024 / 1024, 1)
    info["memory_percent"] = round(memory_info.percent, 1)

    ## Disk.
    disk_total = []
    disk_used = []
    partitions_info = psutil_disk_partitions()
    for partition_info in partitions_info:
        try:
            partition_usage_info = psutil_disk_usage(partition_info.device)
        except PermissionError:
            continue
        disk_total.append(partition_usage_info.total)
        disk_used.append(partition_usage_info.used)
    disk_total = sum(disk_total)
    disk_used = sum(disk_used)
    info["disk_total"] = round(disk_total / 1024 / 1024 / 1024, 1)
    info["disk_percent"] = round(disk_used / disk_total * 100, 1)

    ## Process.
    pids = psutil_pids()
    info["process_count"] = len(pids)

    ## Network.
    net_info = psutil_net_connections()
    info["network_count"] = len(net_info)

    ## User.
    users_info = psutil_users()
    info["login_users"] = [
        {
            "time": datetime.fromtimestamp(
                user_info.started
            ).strftime(
                "%Y-%m-%d %H:%M:%S"
            ),
            "name": user_info.name,
            "host": user_info.host
        }
        for user_info in users_info
    ]
    sort_func = lambda row: row["time"]
    info["login_users"].sort(key=sort_func, reverse=True)

    return info


def get_network_table() -> List[
    Dict[
        Literal["family", "socket", "local_ip", "local_port", "remote_ip", "remote_port", "status", "pid"],
        Any
    ]
]:
    """
    Get network information table.

    Returns
    -------
    Network information table.
    """

    # Get.
    connections = psutil_net_connections("all")
    table = [
        {
            "family": (
                "IPv4"
                if connection.family.name == "AF_INET"
                else "IPv6"
                if connection.family.name == "AF_INET6"
                else None
            ),
            "socket": (
                "TCP"
                if connection.type.name == "SOCK_STREAM"
                else "UDP"
                if connection.type.name == "SOCK_DGRAM"
                else None
            ),
            "local_ip": connection.laddr.ip,
            "local_port": connection.laddr.port,
            "remote_ip": (
                None
                if connection.raddr == ()
                else connection.raddr.ip
            ),
            "remote_port": (
                None
                if connection.raddr == ()
                else connection.raddr.port
            ),
            "status": (
                None
                if connection.status == "NONE"
                else connection.status.lower()
            ),
            "pid": connection.pid
        }
        for connection in connections
    ]

    # Sort.
    sort_func = lambda row: row["local_port"]
    table.sort(key=sort_func)
    sort_func = lambda row: row["local_ip"]
    table.sort(key=sort_func)

    return table


def get_process_table() -> List[
    Dict[
        Literal["create_time", "id", "name", "ports"],
        Any
    ]
]:
    """
    Get process information table.

    Returns
    -------
    Process information table.
    """

    # Get.
    process_iter = psutil_process_iter()
    table = []
    for process in process_iter:
        info = {}
        with process.oneshot():
            info["create_time"] = datetime.fromtimestamp(
                process.create_time()
            ).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
            info["id"] = process.pid
            info["name"] = process.name()
            connections = process.connections()
            if connections == []:
                info["ports"] = None
            else:
                info["ports"] = [
                    connection.laddr.port
                    for connection in connections
                ]
            table.append(info)

    # Sort.
    sort_func = lambda row: row["id"]
    table.sort(key=sort_func)
    sort_func = lambda row: row["create_time"]
    table.sort(key=sort_func)

    return table


def search_process(
    id_: Optional[Union[int, Sequence[int]]] = None,
    name: Optional[Union[str, Sequence[str]]] = None,
    port: Optional[Union[str, int, Sequence[Union[str, int]]]] = None,
) -> List[Process]:
    """
    Search process by ID or name or port.

    Parameters
    ----------
    id_ : Search condition, a value or sequence of process ID.
    name : Search condition, a value or sequence of process name.
    port : Search condition, a value or sequence of process port.

    Returns
    -------
    List of process instances that match any condition.
    """

    # Handle parameter.
    if id_ is None:
        ids = []
    elif id_.__class__ == int:
        ids = [id_]
    else:
        ids = id_
    if name is None:
        names = []
    elif name.__class__ == str:
        names = [name]
    else:
        names = name
    if port is None:
        ports = []
    elif port.__class__ in (str, int):
        ports = [port]
    else:
        ports = port
    ports = [
        int(port)
        for port in ports
    ]

    # Search.
    processes = []
    if (
        names != []
        or ports != []
    ):
        table = get_process_table()
    else:
        table = []

    ## ID.
    for id__ in ids:
        if psutil_pid_exists(id__):
            process = Process(id__)
            processes.append(process)

    ## Name.
    for info in table:
        if (
            info["name"] in names
            and psutil_pid_exists(info["id"])
        ):
            process = Process(info["id"])
            processes.append(process)

    ## Port.
    for info in table:
        for port in ports:
            if (
                info["ports"] is not None
                and port in info["ports"]
                and psutil_pid_exists(info["id"])
            ):
                process = Process(info["id"])
                processes.append(process)
                break

    return processes


def kill_process(
    id_: Optional[Union[int, Sequence[int]]] = None,
    name: Optional[Union[str, Sequence[str]]] = None,
    port: Optional[Union[str, int, Sequence[Union[str, int]]]] = None,
) -> List[Process]:
    """
    Search and kill process by ID or name or port.

    Parameters
    ----------
    id_ : Search condition, a value or sequence of process ID.
    name : Search condition, a value or sequence of process name.
    port : Search condition, a value or sequence of process port.

    Returns
    -------
    List of process instances that match any condition.
    """

    # Get parameter.
    self_pid = os_getpid()

    # Search.
    processes = search_process(id_, name, port)

    # Start.
    for process in processes:
        with process.oneshot():

            ## Filter self process.
            if process.pid == self_pid:
                continue

            process.kill()

    return processes


def stop_process(
    id_: Optional[Union[int, Sequence[int]]] = None,
    name: Optional[Union[str, Sequence[str]]] = None,
    port: Optional[Union[str, int, Sequence[Union[str, int]]]] = None,
) -> List[Process]:
    """
    Search and stop process by ID or name or port.

    Parameters
    ----------
    id_ : Search condition, a value or sequence of process ID.
    name : Search condition, a value or sequence of process name.
    port : Search condition, a value or sequence of process port.

    Returns
    -------
    List of process instances that match any condition.
    """

    # Get parameter.
    self_pid = os_getpid()

    # Search.
    processes = search_process(id_, name, port)

    # Start.
    for process in processes:
        with process.oneshot():

            ## Filter self process.
            if process.pid == self_pid:
                continue

            process.suspend()

    return processes


def start_process(
    id_: Optional[Union[int, Sequence[int]]] = None,
    name: Optional[Union[str, Sequence[str]]] = None,
    port: Optional[Union[str, int, Sequence[Union[str, int]]]] = None,
) -> List[Process]:
    """
    Search and start process by ID or name or port.

    Parameters
    ----------
    id_ : Search condition, a value or sequence of process ID.
    name : Search condition, a value or sequence of process name.
    port : Search condition, a value or sequence of process port.

    Returns
    -------
    List of process instances that match any condition.
    """

    # Search.
    processes = search_process(id_, name, port)

    # Start.
    for process in processes:
        with process.oneshot():
            process.resume()

    return processes


def get_idle_port(min: int = 49152) -> int:
    """
    judge and get an idle port number.

    Parameters
    ----------
    min : Minimum port number.

    Returns
    -------
    Idle port number.
    """

    # Get parameter.
    network_table = get_network_table()
    ports = [
        info["local_port"]
        for info in network_table
    ]

    # Judge.
    while True:
        if min in ports:
            min += 1
        else:
            return min