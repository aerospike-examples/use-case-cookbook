package com.aerospike.examples;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import com.aerospike.client.sdk.Cluster;
import com.aerospike.client.sdk.ClusterDefinition;
import com.aerospike.client.sdk.Host;

/**
 * A small abstraction to parse command line options pertaining to connecting to the cluster
 * and turn them into a {@link Cluster}.
 * <p/>
 * This is a from-scratch port of {@code AerospikeConnector} (see ../java) onto the SDK's
 * {@link ClusterDefinition}/{@link Cluster} API. TLS and non-INTERNAL auth modes aren't
 * wired up yet - only host list and INTERNAL username/password auth are supported so far.
 */
public class SdkConnector {

    private String hosts = "localhost:3000";
    private String userName;
    private String password;
    private String clusterName;
    private boolean servicesAlternate;

    /**
     * Return the <code>Options</code> object with the parameters pre-specified to
     * allow connection to an Aerospike database. This object can be amended with
     * other user-desired options
     *
     * @return An <code>Options</code> object
     */
    public Options getOptions() {
        Options options = new Options();
        options.addOption("h", "hosts", true,
                "List of seed hosts for cluster in format: hostname1[:tlsname][:port1],...\n"
                        + "If the port is not specified, the default port is used.\n"
                        + "Default: localhost:3000\n");
        options.addOption("U", "user", true, "User name for cluster");
        options.addOption("P", "password", true, "Password for cluster");
        options.addOption("cn", "clusterName", true, "Set the cluster name");
        options.addOption("sa", "useServicesAlternate", false,
                "Use services alternative when connecting to the cluster");
        return options;
    }

    /**
     * Validate the passed options as they pertain to connecting to the cluster. If there are
     * validation errors, the first one is returned as a string. If <code>null</code> is
     * returned the connection parameters are valid.
     * <p/>
     * Note that this method does not attempt to connect to the cluster. Use {@link #connect()}
     * for that.
     *
     * @param cl - the command line containing the passed options
     * @return
     */
    public String validateConnectionsOptions(CommandLine cl) {
        this.hosts = cl.getOptionValue("hosts", "localhost:3000");
        this.userName = cl.getOptionValue("user");
        this.password = cl.getOptionValue("password");
        this.clusterName = cl.getOptionValue("clusterName");
        this.servicesAlternate = cl.hasOption("useServicesAlternate");

        if (this.hosts == null) {
            return "Hosts must be specified";
        }

        if (this.userName != null && this.password == null) {
            java.io.Console console = System.console();
            if (console != null) {
                char[] pass = console.readPassword("Enter password for cluster: ");
                if (pass != null) {
                    this.password = new String(pass);
                }
            }
        }
        if (this.userName != null && this.password == null) {
            return "Password must be specified if a username is passed";
        }
        return null;
    }

    /**
     * Connect to the Aerospike cluster using options specified on the command line.
     *
     * @return A connection to the Aerospike cluster
     */
    public Cluster connect() {
        Host[] parsedHosts = Host.parseHosts(this.hosts, 3000);
        ClusterDefinition def = new ClusterDefinition(parsedHosts);

        if (this.userName != null) {
            def.withNativeCredentials(this.userName, this.password);
        }
        if (this.clusterName != null) {
            def.clusterName(this.clusterName);
        }
        if (this.servicesAlternate) {
            def.usingServicesAlternate();
        }
        return def.connect();
    }

    public String getHosts() {
        return hosts;
    }

    public String getUserName() {
        return userName;
    }

    public String getPassword() {
        return password;
    }

    public String getClusterName() {
        return clusterName;
    }

    public boolean isServicesAlternate() {
        return servicesAlternate;
    }
}