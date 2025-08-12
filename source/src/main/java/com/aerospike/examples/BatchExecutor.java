package com.aerospike.examples;

import java.util.Arrays;
import java.util.Optional;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.mapper.tools.AeroMapper;

/**
 * Handles batch execution of use cases from command line arguments.
 * This class follows the Single Responsibility Principle by only handling batch execution.
 */
public class BatchExecutor {
    
    /**
     * Execute a use case specified by name from command line arguments.
     * @param useCaseName The name of the use case to execute
     * @param cl The command line arguments
     * @param client The Aerospike client
     * @param mapper The AeroMapper
     * @return true if execution was successful, false otherwise
     */
    public static boolean executeUseCaseByName(String useCaseName, CommandLine cl, 
            IAerospikeClient client, AeroMapper mapper) {
        
        // Try exact name match first
        Optional<UseCase> useCaseOpt = UseCaseRegistry.findByName(useCaseName);
        
        // If not found, try partial name match
        if (useCaseOpt.isEmpty()) {
            useCaseOpt = UseCaseRegistry.findByPartialName(useCaseName);
        }
        
        if (useCaseOpt.isEmpty()) {
            System.err.println("Error: Use case '" + useCaseName + "' not found.");
            System.err.println("Available use cases:");
            for (UseCase uc : UseCaseRegistry.getAllUseCases()) {
                System.err.println("  " + uc.getName());
            }
            return false;
        }
        
        UseCase useCase = useCaseOpt.get();
        
        // Parse and apply parameters from command line
        if (!ParameterParser.parseAndApplyParameters(useCase, cl)) {
            return false;
        }
        
        // Execute the use case
        UseCaseExecutor executor = new UseCaseExecutor(client, mapper);
        return executor.executeUseCase(useCase, false); // false = non-interactive
    }
    
    /**
     * Add command line options for batch execution.
     * @param options The Options object to add to
     */
    public static void addBatchOptions(Options options) {
        options.addOption("uc", "useCaseName", true, "The name of the use case to run. Partial names are allowed");
        options.addOption("l", "listUseCases", false, "Show a list of all the use cases and their required parameters");
        options.addOption("p", "parameters", false, "Show parameters for a specific use case (use with -uc)");
        
        // Add dynamic parameter options for all use cases
        for (UseCase useCase : UseCaseRegistry.getAllUseCases()) {
            for (Parameter<?> param : useCase.getParams()) {
                String optionName = "param." + param.getName().toLowerCase();
                options.addOption(null, optionName, true, "Parameter: " + param.getName() + " - " + 
                    (param.getDescription() != null ? param.getDescription() : "No description"));
            }
        }
    }
    
    /**
     * Handle batch-specific commands like listing use cases.
     * @param cl The command line arguments
     * @return true if a batch command was handled, false if no batch command was found
     */
    public static boolean handleBatchCommands(CommandLine cl) {
        if (cl.hasOption("listUseCases")) {
            listUseCasesAndParams();
            return true;
        }
        
        if (cl.hasOption("parameters") && cl.hasOption("useCaseName")) {
            String useCaseName = cl.getOptionValue("useCaseName");
            Optional<UseCase> useCaseOpt = UseCaseRegistry.findByName(useCaseName);
            if (useCaseOpt.isEmpty()) {
                useCaseOpt = UseCaseRegistry.findByPartialName(useCaseName);
            }
            
            if (useCaseOpt.isPresent()) {
                ParameterParser.displayParameters(useCaseOpt.get());
            } else {
                System.err.println("Error: Use case '" + useCaseName + "' not found.");
            }
            return true;
        }
        
        return false;
    }
    
    /**
     * List all use cases and their parameters.
     */
    public static void listUseCasesAndParams() {
        System.out.println("Use cases:");
        for (UseCase thisUseCase : UseCaseRegistry.getAllUseCases()) {
            System.out.println("   " + thisUseCase.getName());
            if (thisUseCase.getParams() != null && thisUseCase.getParams().length > 0) {
                System.out.println("      Parameters:");
                for (Parameter<?> param : thisUseCase.getParams()) {
                    System.out.printf("      - %s (default: %s) %s%n", 
                        param.getName(), param.get(), 
                        param.getDescription() != null ? param.getDescription() : "");
                }
            }
        }
        System.out.println();
    }
    
    /**
     * Make sure parameters can be passed case insensitively
     */
    public static String[] correctParamNames(String[] args) {
        return Arrays.stream(args)
                .map(arg -> arg.startsWith("--param.") ? arg.toLowerCase() : arg)
                .toArray(String[]::new);
    }
} 