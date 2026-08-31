package com.aerospike.examples;

import java.util.Scanner;

import com.aerospike.client.sdk.Session;

/**
 * Executes use cases with parameter configuration.
 * <p/>
 * Port of ../../java's {@code UseCaseExecutor}: {@code Session} replaces the legacy
 * {@code IAerospikeClient}/{@code AeroMapper} pair everywhere, since the SDK's {@code Session}
 * already exposes both raw record operations and object mapping. Parameter handling logic is
 * otherwise identical - {@code Parameter<T>} is unchanged between the two modules.
 * <p/>
 * Takes a {@code Scanner} in its constructor rather than creating its own per parameter-edit
 * call: a fresh {@code Scanner} wrapping the same {@code System.in} stream silently drops
 * whatever the previous one had already buffered, which swallows queued commands under piped or
 * scripted stdin. Sharing one {@code Scanner} with {@link InteractiveMenu} avoids that.
 */
public class UseCaseExecutor {

    private final Session session;
    private final Scanner scanner;

    public UseCaseExecutor(Session session, Scanner scanner) {
        this.session = session;
        this.scanner = scanner;
    }

    /**
     * Execute a use case with optional parameter configuration.
     * @param useCase The use case to execute
     * @param interactive Whether to allow interactive parameter configuration
     * @return true if execution was successful, false if cancelled
     */
    public boolean executeUseCase(UseCase useCase, boolean interactive, boolean seedOnly, boolean runOnly) {
        System.out.println("\n" + AnsiColors.BOLD + "Executing Use Case: " + useCase.getName() + AnsiColors.RESET + "\n");

        Parameter<?>[] params = useCase.getParams();
        if (params.length > 0) {
            if (interactive) {
                if (!handleParameters(useCase, params)) {
                    return false;
                }
            }
            else {
                System.out.println(AnsiColors.CYAN + "Using default parameters:" + AnsiColors.RESET);
                for (Parameter<?> param : params) {
                    System.out.printf("  %s = %s%n", param.getName(), param.get());
                }
                System.out.println();
            }
        }

        try {
            if (!runOnly) {
                System.out.println("\nSetting up the data for the use case...");
                useCase.setup(session);
            }
            if (!seedOnly) {
                System.out.println("\nExecuting the use case...");
                useCase.run(session);
            }
            System.out.println(AnsiColors.GREEN + "\nUse case completed successfully!" + AnsiColors.RESET);
            return true;
        }
        catch (Exception e) {
            System.err.println(AnsiColors.RED + "An error occurred during execution of the use case. The error details are:" + AnsiColors.RESET);
            System.err.println("   Message: " + e.getMessage());
            System.err.println("   Class: " + e.getClass().getName());
            System.err.println("   Stack trace:");
            e.printStackTrace();
            return false;
        }
    }

    /**
     * Handles parameter configuration for a use case.
     * @param uc The use case
     * @param params The parameters to configure
     * @return true if the user wants to proceed, false if they cancelled
     */
    private boolean handleParameters(UseCase uc, Parameter<?>[] params) {
        System.out.println(AnsiColors.CYAN + "This use case has configurable parameters:" + AnsiColors.RESET);
        System.out.println();

        for (int i = 0; i < params.length; i++) {
            Parameter<?> param = params[i];
            String description = param.getDescription();
            if (description == null) {
                description = "No description available";
            }
            System.out.printf("%s%d.%s %s%s%s = %s%s%s%n",
                    AnsiColors.YELLOW, i + 1, AnsiColors.RESET,
                    AnsiColors.BOLD, param.getName(), AnsiColors.RESET,
                    AnsiColors.GREEN, param.get(), AnsiColors.RESET);
            System.out.printf("   %s%s%s%n", AnsiColors.MEDIUM_GRAY, description, AnsiColors.RESET);
        }

        System.out.println();
        System.out.println("Options:");
        System.out.println("  " + AnsiColors.YELLOW + "Enter" + AnsiColors.RESET + " - Use current parameters and run the use case");
        System.out.println("  " + AnsiColors.YELLOW + "1-" + params.length + AnsiColors.RESET + " - Modify a specific parameter");
        System.out.println("  " + AnsiColors.YELLOW + "cancel" + AnsiColors.RESET + " - Cancel and return to menu");
        System.out.println();

        while (true) {
            System.out.print(AnsiColors.CYAN + "Enter your choice: " + AnsiColors.RESET);
            String input = scanner.nextLine().trim();

            if (input.isEmpty()) {
                return true;
            }

            if (input.equalsIgnoreCase("cancel")) {
                return false;
            }

            try {
                int paramIndex = Integer.parseInt(input) - 1;
                if (paramIndex >= 0 && paramIndex < params.length) {
                    if (modifyParameter(params[paramIndex])) {
                        System.out.println();
                        System.out.println(AnsiColors.CYAN + "Updated parameters:" + AnsiColors.RESET);
                        for (int i = 0; i < params.length; i++) {
                            Parameter<?> param = params[i];
                            String description = param.getDescription();
                            if (description == null) {
                                description = "No description available";
                            }
                            System.out.printf("%s%d.%s %s%s%s = %s%s%s%n",
                                    AnsiColors.YELLOW, i + 1, AnsiColors.RESET,
                                    AnsiColors.BOLD, param.getName(), AnsiColors.RESET,
                                    AnsiColors.GREEN, param.get(), AnsiColors.RESET);
                            System.out.printf("   %s%s%s%n", AnsiColors.MEDIUM_GRAY, description, AnsiColors.RESET);
                        }
                        System.out.println();
                    }
                }
                else {
                    System.out.println(AnsiColors.RED + "Invalid parameter number. Please enter 1-" + params.length + " or 'cancel'." + AnsiColors.RESET);
                }
            }
            catch (NumberFormatException e) {
                System.out.println(AnsiColors.RED + "Invalid input. Please enter a number, 'cancel', or press Enter to use current parameters." + AnsiColors.RESET);
            }
        }
    }

    /**
     * Modifies a single parameter value.
     * @param param The parameter to modify
     * @return true if the parameter was modified, false if cancelled
     */
    private boolean modifyParameter(Parameter<?> param) {
        System.out.println();
        System.out.printf("Modifying parameter: %s%s%s%n", AnsiColors.BOLD, param.getName(), AnsiColors.RESET);
        if (param.getDescription() != null) {
            System.out.printf("Description: %s%s%s%n", AnsiColors.MEDIUM_GRAY, param.getDescription(), AnsiColors.RESET);
        }
        System.out.printf("Current value: %s%s%s%n", AnsiColors.GREEN, param.get(), AnsiColors.RESET);
        System.out.printf("Type: %s%s%s%n", AnsiColors.YELLOW, param.get().getClass().getSimpleName(), AnsiColors.RESET);
        System.out.println();
        System.out.println("Enter new value or 'cancel' to keep current value:");

        while (true) {
            System.out.print(AnsiColors.CYAN + "New value: " + AnsiColors.RESET);
            String input = scanner.nextLine().trim();

            if (input.equalsIgnoreCase("cancel")) {
                return false;
            }

            try {
                Object newValue = parseValue(input, param.get().getClass());
                setParameterValue(param, newValue);
                System.out.println(AnsiColors.GREEN + "Parameter updated successfully!" + AnsiColors.RESET);
                return true;
            }
            catch (IllegalArgumentException e) {
                System.out.println(AnsiColors.RED + "Invalid value: " + e.getMessage() + AnsiColors.RESET);
                System.out.println("Please enter a valid " + param.get().getClass().getSimpleName() + " value or 'cancel'.");
            }
        }
    }

    /**
     * Parses a string value to the appropriate type.
     * @param input The input string
     * @param targetType The target type
     * @return The parsed value
     * @throws IllegalArgumentException if the value cannot be parsed
     */
    private Object parseValue(String input, Class<?> targetType) {
        if (targetType == String.class) {
            return input;
        }
        else if (targetType == Integer.class || targetType == int.class) {
            return Integer.parseInt(input);
        }
        else if (targetType == Long.class || targetType == long.class) {
            return Long.parseLong(input);
        }
        else if (targetType == Double.class || targetType == double.class) {
            return Double.parseDouble(input);
        }
        else if (targetType == Float.class || targetType == float.class) {
            return Float.parseFloat(input);
        }
        else if (targetType == Boolean.class || targetType == boolean.class) {
            return Boolean.parseBoolean(input);
        }
        else {
            throw new IllegalArgumentException("Unsupported type: " + targetType.getSimpleName());
        }
    }

    /**
     * Sets the value of a parameter using reflection.
     * @param param The parameter
     * @param value The new value
     */
    private void setParameterValue(Parameter<?> param, Object value) {
        try {
            java.lang.reflect.Field valueField = Parameter.class.getDeclaredField("value");
            valueField.setAccessible(true);
            valueField.set(param, value);
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to set parameter value", e);
        }
    }
}
