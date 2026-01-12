package com.aerospike.examples;

import java.util.List;
import java.util.Optional;

import com.aerospike.examples.advancedexpressions.AdvancedExpressions;
import com.aerospike.examples.gaming.Leaderboard;
import com.aerospike.examples.gaming.PlayerMatching;
import com.aerospike.examples.manytomany.ManyToManyRelationships;
import com.aerospike.examples.onetomany.OneToManyRelationships;
import com.aerospike.examples.recordversioning.VersioningRecords;
import com.aerospike.examples.setup.SetupDemo;
import com.aerospike.examples.timeseries.TimeSeriesDemo;
import com.aerospike.examples.timeseries.TimeSeriesLargeVarianceDemo;
import com.aerospike.examples.transactionprocessing.TopTransactionsAcrossDcs;

/**
 * Registry for managing all available use cases.
 * This class follows the Single Responsibility Principle by only managing use case registration and lookup.
 */
public class UseCaseRegistry {
    
    private static final List<UseCase> USE_CASES = List.of(
            new SetupDemo(),
            new OneToManyRelationships(),
            new ManyToManyRelationships(),
            new Leaderboard(),
            new PlayerMatching(),
            new TimeSeriesDemo(),
            new TimeSeriesLargeVarianceDemo(),
            new TopTransactionsAcrossDcs(),
            new VersioningRecords(),
            new AdvancedExpressions()
    );
    
    /**
     * Get all registered use cases.
     * @return List of all use cases
     */
    public static List<UseCase> getAllUseCases() {
        return USE_CASES;
    }
    
    /**
     * Find a use case by name (case-insensitive).
     * @param name The name to search for
     * @return Optional containing the use case if found
     */
    public static Optional<UseCase> findByName(String name) {
        return USE_CASES.stream()
                .filter(uc -> uc.getName().equalsIgnoreCase(name))
                .findFirst();
    }
    
    /**
     * Find a use case by partial name match (case-insensitive).
     * @param partialName The partial name to search for
     * @return Optional containing the use case if found
     */
    public static Optional<UseCase> findByPartialName(String partialName) {
        return USE_CASES.stream()
                .filter(uc -> uc.getName().toLowerCase().contains(partialName.toLowerCase()))
                .findFirst();
    }
    
    /**
     * Get a use case by index.
     * @param index The index (0-based)
     * @return Optional containing the use case if index is valid
     */
    public static Optional<UseCase> getByIndex(int index) {
        if (index >= 0 && index < USE_CASES.size()) {
            return Optional.of(USE_CASES.get(index));
        }
        return Optional.empty();
    }
    
    /**
     * Get the total number of use cases.
     * @return Number of use cases
     */
    public static int getUseCaseCount() {
        return USE_CASES.size();
    }
} 