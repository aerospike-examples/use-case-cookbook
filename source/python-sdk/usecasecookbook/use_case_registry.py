"""Single source of truth for what appears in the menu, batch mode, and --listUseCases.
Mirrors ../../java-sdk's UseCaseRegistry.
"""


from usecasecookbook.use_case import UseCase

_USE_CASES: list[UseCase] = []


def _register() -> list[UseCase]:
    from usecasecookbook.advancedexpressions.advanced_expressions import (
        AdvancedExpressions,
    )
    from usecasecookbook.gaming.leaderboard import Leaderboard
    from usecasecookbook.gaming.player_matching import PlayerMatching
    from usecasecookbook.hotkeys.read_hot_key import ReadHotKeyUseCase
    from usecasecookbook.hotkeys.reducer_hot_key import ReducerHotKeyUseCase
    from usecasecookbook.hotkeys.write_hot_key import WriteHotKeyUseCase
    from usecasecookbook.manytomany.many_to_many_relationships import (
        ManyToManyRelationships,
    )
    from usecasecookbook.onetomany.one_to_many_relationships import (
        OneToManyRelationships,
    )
    from usecasecookbook.recordversioning.delta_versioning_records import (
        DeltaVersioningRecords,
    )
    from usecasecookbook.recordversioning.versioning_records import VersioningRecords
    from usecasecookbook.setup.setup_demo import SetupDemo
    from usecasecookbook.timeseries.time_series_demo import TimeSeriesDemo
    from usecasecookbook.timeseries.time_series_large_variance_demo import (
        TimeSeriesLargeVarianceDemo,
    )
    from usecasecookbook.transactionprocessing.top_transactions_across_dcs import (
        TopTransactionsAcrossDcs,
    )

    return [
        SetupDemo(),
        OneToManyRelationships(),
        ManyToManyRelationships(),
        Leaderboard(),
        PlayerMatching(),
        TimeSeriesDemo(),
        TimeSeriesLargeVarianceDemo(),
        TopTransactionsAcrossDcs(),
        VersioningRecords(),
        DeltaVersioningRecords(),
        ReadHotKeyUseCase(),
        WriteHotKeyUseCase(),
        ReducerHotKeyUseCase(),
        AdvancedExpressions(),
    ]


def get_all_use_cases() -> list[UseCase]:
    global _USE_CASES
    if not _USE_CASES:
        _USE_CASES = _register()
    return _USE_CASES


def find_by_name(name: str) -> UseCase | None:
    for uc in get_all_use_cases():
        if uc.get_name().lower() == name.lower():
            return uc
    return None


def find_all_by_partial_name(partial_name: str) -> list[UseCase]:
    needle = partial_name.lower()
    return [uc for uc in get_all_use_cases() if needle in uc.get_name().lower()]


def find_by_partial_name(partial_name: str) -> UseCase | None:
    matches = find_all_by_partial_name(partial_name)
    return matches[0] if len(matches) == 1 else None


def get_by_index(index: int) -> UseCase | None:
    use_cases = get_all_use_cases()
    if 0 <= index < len(use_cases):
        return use_cases[index]
    return None


def get_use_case_count() -> int:
    return len(get_all_use_cases())
