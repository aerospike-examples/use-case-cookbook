#!/usr/bin/env python3
"""Main entry point for the Python SDK port of the Use Case Cookbook.

Mirrors ../java-sdk/src/main/java/com/aerospike/examples/UseCaseCookbookRunner.java:
supports both a named-use-case batch path (``-uc``) and the interactive menu (no ``-uc``).
Cluster capability detection uses ``SyncSession.is_namespace_sc`` directly (see
usecasecookbook/txn.py) rather than a throwaway-transaction probe, since this SDK exposes
strong-consistency status as a plain query.
"""

import argparse
import sys
from typing import List, Optional

from usecasecookbook import ansi_colors as c
from usecasecookbook import config
from usecasecookbook import connector
from usecasecookbook import use_case_registry as registry
from usecasecookbook.interactive_menu import InteractiveMenu
from usecasecookbook.parameter import Parameter
from usecasecookbook.txn import transactions_supported
from usecasecookbook.use_case import UseCase
from usecasecookbook.use_case_executor import UseCaseExecutor


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Aerospike Use Case Cookbook (Python SDK)",
        add_help=False,
    )
    connector.add_connection_arguments(parser)
    parser.add_argument(
        "-uc", "--useCaseName",
        help="The name of the use case to run. Partial names are allowed. If omitted, an interactive menu is shown",
    )
    parser.add_argument("-l", "--listUseCases", action="store_true", help="Show a list of all the use cases")
    parser.add_argument("-ro", "--runOnly", action="store_true", help="Only execute the use case, do not seed data for it")
    parser.add_argument("-so", "--seedOnly", action="store_true", help="Only seed (generate) the data, do not execute the use case")
    parser.add_argument("-?", "--help", action="help", help="Show this message")
    return parser


def list_use_cases() -> None:
    print("Use cases:")
    for uc in registry.get_all_use_cases():
        print(f"   {uc.get_name()}")


def apply_param_overrides(use_case: UseCase, overrides: dict) -> Optional[str]:
    """Applies ``--param.<name>=<value>`` overrides onto ``use_case``'s parameters.
    Returns an error message if a name/type doesn't match, else ``None``.
    """
    params_by_name = {p.name.lower(): p for p in use_case.get_params()}
    for name, raw_value in overrides.items():
        param: Optional[Parameter] = params_by_name.get(name.lower())
        if param is None:
            return f"Use case '{use_case.get_name()}' has no parameter named '{name}'"
        value_type = type(param.value)
        try:
            if value_type is bool:
                param.value = raw_value.strip().lower() in ("true", "1", "yes")
            else:
                param.value = value_type(raw_value)
        except ValueError:
            return f"Could not parse '{raw_value}' as {value_type.__name__} for parameter '{name}'"
    return None


def parse_param_overrides(argv: List[str]) -> dict:
    overrides = {}
    for arg in argv:
        if arg.startswith("--param.") and "=" in arg:
            key, value = arg[len("--param."):].split("=", 1)
            overrides[key] = value
    return overrides


def execute_use_case_by_name(
    use_case_name: str, executor: UseCaseExecutor, seed_only: bool, run_only: bool, overrides: dict,
) -> bool:
    use_case = registry.find_by_name(use_case_name)
    if use_case is None:
        matches = registry.find_all_by_partial_name(use_case_name)
        if len(matches) == 1:
            use_case = matches[0]
        elif len(matches) > 1:
            print(f"Error: '{use_case_name}' matches more than one use case - be more specific:", file=sys.stderr)
            for m in matches:
                print(f"   {m.get_name()}", file=sys.stderr)
            return False

    if use_case is None:
        print(f"Error: Use case '{use_case_name}' not found.", file=sys.stderr)
        list_use_cases()
        return False

    if overrides:
        error = apply_param_overrides(use_case, overrides)
        if error:
            print(f"Error: {error}", file=sys.stderr)
            return False

    return executor.execute_use_case(use_case, interactive=False, seed_only=seed_only, run_only=run_only)


def main() -> int:
    parser = build_arg_parser()
    args, remaining = parser.parse_known_args()
    param_overrides = parse_param_overrides(remaining)

    if args.listUseCases:
        list_use_cases()
        return 0

    error = connector.validate_connection_options(args)
    if error:
        print(error)
        parser.print_help()
        return 1

    if args.runOnly and args.seedOnly:
        print("Both 'runOnly' and 'seedOnly' cannot be specified.")
        parser.print_help()
        return 1

    if args.useCaseName is not None and not args.useCaseName.strip():
        print("The use case name (-uc) cannot be blank.")
        parser.print_help()
        return 1

    client = connector.connect(args)
    try:
        session = client.create_session()

        if not transactions_supported(session):
            print(
                f"{c.YELLOW}Note: namespace '{config.NAMESPACE}' does not support multi-record "
                "transactions (requires Aerospike 8+ with a strong-consistency-enabled "
                "namespace). Transaction-based use cases will still run, but without the "
                f"atomicity guarantee a real transaction would provide.{c.RESET}"
            )

        if args.useCaseName:
            executor = UseCaseExecutor(session)
            success = execute_use_case_by_name(
                args.useCaseName, executor, args.seedOnly, args.runOnly, param_overrides,
            )
            return 0 if success else 1

        InteractiveMenu(session).run_menu()
        return 0
    finally:
        client.close()


if __name__ == "__main__":
    sys.exit(main())
