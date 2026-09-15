"""Handles the interactive menu system for use case selection and execution.
Mirrors ../../java-sdk's InteractiveMenu - see ../java/README_SEARCH.md for the search
command reference, which applies unchanged here.
"""

import re
import shutil

from aerospike_sdk import SyncSession

from usecasecookbook import ansi_colors as c
from usecasecookbook import use_case_registry as registry
from usecasecookbook.use_case import UseCase
from usecasecookbook.use_case_executor import UseCaseExecutor


class InteractiveMenu:
    def __init__(self, session: SyncSession):
        self.executor = UseCaseExecutor(session)
        self.filtered_use_cases: list[UseCase] = list(registry.get_all_use_cases())
        self.current_search_term: str | None = None
        self.is_regex_search = False

    def run_menu(self, width: int | None = None) -> None:
        if width is None:
            width = shutil.get_terminal_size(fallback=(200, 24)).columns

        summary = False
        skip_use_case_details = False

        while True:
            self._show_start_header()
            if not skip_use_case_details:
                self._show_use_case_details(width, summary)
            skip_use_case_details = False

            if self.current_search_term and self.current_search_term.strip():
                kind = "regex '" if self.is_regex_search else "'"
                print(
                    f"{c.YELLOW}Search active: {kind}{self.current_search_term}' "
                    f"({len(self.filtered_use_cases)} results){c.RESET}"
                )

            user_input = input("Enter a number to run a use case, or enter a command (help for commands): ").strip()

            if not user_input:
                continue

            lowered = user_input.lower()
            if lowered in ("help", "h", "?"):
                self._show_help()
                skip_use_case_details = True
                continue

            if lowered == "summary":
                summary = True
                continue

            if lowered in ("full", "details"):
                summary = False
                continue

            if user_input.startswith("search "):
                self._search_use_cases(user_input[7:].strip(), False)
                continue

            if user_input.startswith("s "):
                self._search_use_cases(user_input[2:].strip(), False)
                continue

            if user_input.startswith("/"):
                self._search_use_cases(user_input[1:].strip(), True)
                continue

            if lowered in ("clear", "c"):
                self._search_use_cases(None, False)
                continue

            if lowered in ("exit", "quit"):
                print("Goodbye!")
                break

            try:
                selection = int(user_input)
            except ValueError:
                print(f"{c.RED}Invalid input. Please enter a number or a command (type 'help' for available commands).{c.RESET}")
                continue

            if 1 <= selection <= len(self.filtered_use_cases):
                self._invoke_use_case(selection)
            else:
                print(f"{c.RED}Invalid selection. Please enter a number between 1 and {len(self.filtered_use_cases)}.{c.RESET}")

    @staticmethod
    def _show_start_header() -> None:
        print()
        print(f"{c.BOLD}Aerospike Use Case Cookbook (Python SDK){c.RESET}")
        print("==============================")
        print()

    def _format_colors(self, text: str, color: str) -> str:
        return f"{color}{self._highlight_search_terms(text, color)}{c.RESET}"

    def _format_line(self, index_str: str, name_str: str, desc_str: str, fmt: str) -> str:
        return f"| {self._format_colors(index_str, fmt)} | {self._format_colors(name_str, fmt)} | {self._format_colors(desc_str, fmt)} |"

    @staticmethod
    def _form_tags_string(tags: list[str], color: str) -> str:
        parts = [f"{c.REVERSE}{tag}{c.RESET}{color}" for tag in tags]
        return "Tags: " + " ".join(parts)

    def _show_use_case_details(self, width: int, summary: bool) -> None:
        if not self.filtered_use_cases:
            print(f"{c.RED}No use cases found matching your search criteria.{c.RESET}")
            return

        column_spaces = 6
        column_chars = 4
        longest_name = max((len(uc.get_name()) for uc in self.filtered_use_cases), default=0)
        index_width = len(str(len(self.filtered_use_cases)))
        name_width = min(longest_name, 50)
        desc_width = max(width - index_width - name_width - column_chars - column_spaces, 10)

        index_header = self._pad_center("#", index_width)
        name_header = self._pad_center("Name", name_width)
        desc_header = self._pad_center("Summary" if summary else "Description", desc_width)

        horizontal_line = "-" * width
        print(horizontal_line)
        print(self._format_line(index_header, name_header, desc_header, c.BOLD))
        print(horizontal_line)

        for i, uc in enumerate(self.filtered_use_cases):
            index = str(i + 1).rjust(index_width)
            color = c.COLOR2 if i % 2 == 0 else c.COLOR1
            name = uc.get_name().ljust(name_width)
            wrapped_desc = self._wrap_text(self._form_use_case_text(uc, summary), desc_width)

            for j, desc_line in enumerate(wrapped_desc):
                padded = desc_line.ljust(desc_width)
                if j == 0:
                    print(self._format_line(index, name, padded, color))
                else:
                    print(self._format_line(" " * index_width, " " * name_width, padded, color))

            if uc.get_reference():
                print(self._format_line(" " * index_width, " " * name_width, " " * desc_width, color))
                print(self._format_line(
                    " " * index_width, " " * name_width,
                    f"See: {uc.get_reference()}".ljust(desc_width), color,
                ))
            if uc.get_tags():
                print(self._format_line(
                    " " * index_width, " " * name_width,
                    self._form_tags_string(uc.get_tags(), color).ljust(desc_width), color,
                ))
            print(horizontal_line)

    @staticmethod
    def _form_use_case_text(uc: UseCase, summary: bool) -> str:
        if summary:
            return uc.get_description().split(".")[0] + "."
        return uc.get_description()

    @staticmethod
    def _wrap_text(text: str, width: int) -> list[str]:
        lines: list[str] = []
        for paragraph in text.split("\n"):
            current_line = ""
            for word in paragraph.split():
                if len(current_line) + len(word) + 1 <= width:
                    current_line = f"{current_line} {word}".strip()
                else:
                    if current_line:
                        lines.append(current_line)
                        current_line = word
                    else:
                        lines.append(word[:width])
                        current_line = word[width:]
            if current_line:
                lines.append(current_line)
        return lines

    @staticmethod
    def _pad_center(text: str, width: int) -> str:
        left = max(0, (width - len(text)) // 2)
        right = max(0, width - left - len(text))
        return " " * left + text + " " * right

    def _highlight_search_terms(self, text: str, preserve_color: str) -> str:
        if not self.current_search_term or not self.current_search_term.strip():
            return text
        try:
            pattern = re.compile(
                self.current_search_term if self.is_regex_search else re.escape(self.current_search_term),
                re.IGNORECASE,
            )
        except re.error:
            pattern = re.compile(re.escape(self.current_search_term), re.IGNORECASE)
        return pattern.sub(lambda m: f"{c.HIGHLIGHT}{m.group()}{c.RESET}{preserve_color}", text)

    def _search_use_cases(self, search_term: str | None, use_regex: bool) -> None:
        self.current_search_term = search_term
        self.is_regex_search = use_regex

        if not search_term or not search_term.strip():
            self.filtered_use_cases = list(registry.get_all_use_cases())
            return

        try:
            pattern = re.compile(search_term if use_regex else re.escape(search_term), re.IGNORECASE)
        except re.error:
            pattern = re.compile(re.escape(search_term), re.IGNORECASE)

        self.filtered_use_cases = [uc for uc in registry.get_all_use_cases() if self._matches_search(uc, pattern)]

    @staticmethod
    def _matches_search(use_case: UseCase, pattern: "re.Pattern") -> bool:
        return bool(
            pattern.search(use_case.get_name())
            or pattern.search(use_case.get_description())
            or pattern.search(use_case.get_reference())
            or pattern.search(" ".join(use_case.get_tags()))
        )

    def _invoke_use_case(self, selection: int) -> None:
        uc = self.filtered_use_cases[selection - 1]
        self.executor.execute_use_case(uc, True, False, False)

    @staticmethod
    def _show_help() -> None:
        print("Help commands:")
        print("  help           - Show this help text")
        print("Display commands:")
        print("  summary        - Show only summary (one line) use case descriptions")
        print("  full / details - Show detailed use case descriptions")
        print("Search commands:")
        print("  search <term>  - Search for term in names, descriptions, and URLs")
        print("  s <term>       - Short form of search")
        print("  /<regex>       - Search using regular expression")
        print("  clear          - Clear search and show all use cases")
        print("Running a use case:")
        print("  <shown number> - Execute the use case with the specified number")
        print("                  (If the use case has parameters, you'll be prompted to configure them)")
        print("Exiting the program:")
        print("  exit / quit    - Exit the program")
        print()
