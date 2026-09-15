"""Executes use cases with parameter configuration. Mirrors ../../java-sdk's
UseCaseExecutor - ``Parameter.value`` is a plain public attribute here, so no
reflection trick is needed to mutate it (see parameter.py).
"""

import traceback

from aerospike_sdk import SyncSession

from usecasecookbook import ansi_colors as c
from usecasecookbook.parameter import Parameter
from usecasecookbook.use_case import UseCase

_PARSERS = {
    str: lambda s: s,
    int: int,
    float: float,
    bool: lambda s: s.strip().lower() in ("true", "1", "yes"),
}


class UseCaseExecutor:
    def __init__(self, session: SyncSession):
        self.session = session

    def execute_use_case(
        self, use_case: UseCase, interactive: bool, seed_only: bool, run_only: bool
    ) -> bool:
        print(f"\n{c.BOLD}Executing Use Case: {use_case.get_name()}{c.RESET}\n")

        params = use_case.get_params()
        if params:
            if interactive:
                if not self._handle_parameters(params):
                    return False
            else:
                print(f"{c.CYAN}Using default parameters:{c.RESET}")
                for param in params:
                    print(f"  {param.name} = {param.get()}")
                print()

        try:
            if not run_only:
                print("\nSetting up the data for the use case...")
                use_case.setup(self.session)
            if not seed_only:
                print("\nExecuting the use case...")
                use_case.run(self.session)
            print(f"{c.GREEN}\nUse case completed successfully!{c.RESET}")
            return True
        except Exception as e:  # noqa: BLE001 - deliberately broad, mirrors the Java catch-all
            print(f"{c.RED}An error occurred during execution of the use case. The error details are:{c.RESET}")
            print(f"   Message: {e}")
            print(f"   Class: {type(e).__module__}.{type(e).__qualname__}")
            print("   Stack trace:")
            traceback.print_exc()
            return False

    def _handle_parameters(self, params) -> bool:
        print(f"{c.CYAN}This use case has configurable parameters:{c.RESET}")
        print()
        self._print_params(params)

        print()
        print("Options:")
        print(f"  {c.YELLOW}Enter{c.RESET} - Use current parameters and run the use case")
        print(f"  {c.YELLOW}1-{len(params)}{c.RESET} - Modify a specific parameter")
        print(f"  {c.YELLOW}cancel{c.RESET} - Cancel and return to menu")
        print()

        while True:
            user_input = input(f"{c.CYAN}Enter your choice: {c.RESET}").strip()

            if not user_input:
                return True
            if user_input.lower() == "cancel":
                return False

            try:
                param_index = int(user_input) - 1
            except ValueError:
                print(f"{c.RED}Invalid input. Please enter a number, 'cancel', or press Enter to use current parameters.{c.RESET}")
                continue

            if 0 <= param_index < len(params):
                if self._modify_parameter(params[param_index]):
                    print()
                    print(f"{c.CYAN}Updated parameters:{c.RESET}")
                    self._print_params(params)
                    print()
            else:
                print(f"{c.RED}Invalid parameter number. Please enter 1-{len(params)} or 'cancel'.{c.RESET}")

    @staticmethod
    def _print_params(params) -> None:
        for i, param in enumerate(params):
            description = param.description or "No description available"
            print(f"{c.YELLOW}{i + 1}.{c.RESET} {c.BOLD}{param.name}{c.RESET} = {c.GREEN}{param.get()}{c.RESET}")
            print(f"   {c.MEDIUM_GRAY}{description}{c.RESET}")

    @staticmethod
    def _modify_parameter(param: Parameter) -> bool:
        print()
        print(f"Modifying parameter: {c.BOLD}{param.name}{c.RESET}")
        if param.description:
            print(f"Description: {c.MEDIUM_GRAY}{param.description}{c.RESET}")
        print(f"Current value: {c.GREEN}{param.get()}{c.RESET}")
        value_type = type(param.value)
        print(f"Type: {c.YELLOW}{value_type.__name__}{c.RESET}")
        print()
        print("Enter new value or 'cancel' to keep current value:")

        while True:
            user_input = input(f"{c.CYAN}New value: {c.RESET}").strip()
            if user_input.lower() == "cancel":
                return False

            parser = _PARSERS.get(value_type)
            if parser is None:
                print(f"{c.RED}Invalid value: unsupported type: {value_type.__name__}{c.RESET}")
                continue
            try:
                param.value = parser(user_input)
                print(f"{c.GREEN}Parameter updated successfully!{c.RESET}")
                return True
            except ValueError:
                print(f"{c.RED}Invalid value: could not parse '{user_input}' as {value_type.__name__}{c.RESET}")
                print(f"Please enter a valid {value_type.__name__} value or 'cancel'.")
