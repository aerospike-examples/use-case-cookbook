package com.aerospike.examples;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.cli.CommandLine;

/**
 * Parses and applies command-line parameters to use cases.
 * This class follows the Single Responsibility Principle by only handling parameter parsing.
 */
public class ParameterParser {
    
    /**
     * Parse parameters from command line arguments and apply them to a use case.
     * @param useCase The use case to apply parameters to
     * @param cl The command line arguments
     * @return true if parameters were successfully applied, false if there were errors
     */
    public static boolean parseAndApplyParameters(UseCase useCase, CommandLine cl) {
        Parameter<?>[] params = useCase.getParams();
        if (params.length == 0) {
            return true; // No parameters to parse
        }
        
        Map<String, Parameter<?>> paramMap = new HashMap<>();
        for (Parameter<?> param : params) {
            paramMap.put(param.getName().toLowerCase(), param);
        }
        
        boolean hasErrors = false;
        
        // Check for parameter arguments (format: --paramName=value)
        for (org.apache.commons.cli.Option option : cl.getOptions()) {
            String optionName = option.getLongOpt();
            if (optionName != null && optionName.startsWith("param.")) {
                String paramName = optionName.substring(6); // Remove "param." prefix
                String value = cl.getOptionValue(optionName);
                
                Parameter<?> param = paramMap.get(paramName.toLowerCase());
                if (param != null) {
                    if (!setParameterValue(param, value)) {
                        System.err.println("Error: Invalid value '" + value + "' for parameter '" + paramName + "'");
                        hasErrors = true;
                    }
                } else {
                    System.err.println("Error: Unknown parameter '" + paramName + "' for use case '" + useCase.getName() + "'");
                    hasErrors = true;
                }
            }
        }
        
        return !hasErrors;
    }
    
    /**
     * Set a parameter value from a string.
     * @param param The parameter to set
     * @param value The string value
     * @return true if the value was set successfully, false if there was an error
     */
    private static boolean setParameterValue(Parameter<?> param, String value) {
        try {
            Object parsedValue = parseValue(value, param.get().getClass());
            setParameterValue(param, parsedValue);
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }
    
    /**
     * Parses a string value to the appropriate type.
     * @param input The input string
     * @param targetType The target type
     * @return The parsed value
     * @throws IllegalArgumentException if the value cannot be parsed
     */
    private static Object parseValue(String input, Class<?> targetType) {
        if (targetType == String.class) {
            return input;
        } else if (targetType == Integer.class || targetType == int.class) {
            return Integer.parseInt(input);
        } else if (targetType == Long.class || targetType == long.class) {
            return Long.parseLong(input);
        } else if (targetType == Double.class || targetType == double.class) {
            return Double.parseDouble(input);
        } else if (targetType == Float.class || targetType == float.class) {
            return Float.parseFloat(input);
        } else if (targetType == Boolean.class || targetType == boolean.class) {
            return Boolean.parseBoolean(input);
        } else {
            throw new IllegalArgumentException("Unsupported type: " + targetType.getSimpleName());
        }
    }
    
    /**
     * Sets the value of a parameter using reflection.
     * @param param The parameter
     * @param value The new value
     */
    private static void setParameterValue(Parameter<?> param, Object value) {
        try {
            // Use reflection to access the private 'value' field
            java.lang.reflect.Field valueField = Parameter.class.getDeclaredField("value");
            valueField.setAccessible(true);
            valueField.set(param, value);
        } catch (Exception e) {
            throw new RuntimeException("Failed to set parameter value", e);
        }
    }
    
    /**
     * Display available parameters for a use case.
     * @param useCase The use case
     */
    public static void displayParameters(UseCase useCase) {
        Parameter<?>[] params = useCase.getParams();
        if (params.length == 0) {
            System.out.println("No parameters available for this use case.");
            return;
        }
        
        System.out.println("Available parameters for '" + useCase.getName() + "':");
        for (Parameter<?> param : params) {
            String description = param.getDescription();
            if (description == null) {
                description = "No description available";
            }
            System.out.printf("  --param.%s <value> (default: %s) - %s%n", 
                param.getName(), param.get(), description);
        }
    }
} 