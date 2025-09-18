package com.aerospike.examples;

import java.util.Arrays;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Host;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Log;
import com.aerospike.client.policy.AuthMode;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.TlsPolicy;

/**
 * An abstraction class to parse command line options pertaining to connecting
 * to the cluster.
 * <p/>
 * This class can handle connection to either Aerospike native or Aerospike
 * Cloud. If using Aerospike native, full options such as TLS certificates can
 * be specified. (PEM format, not JKS)
 * <p/>
 * There are 2 ways to use Aerospike Cloud over the default Aerospike client.
 * The most explicit one is to use the <code>--useCloud</code> option. The other
 * is specifying a single host address that ends in <code>.asdb.io</code> which
 * is the domain for Aerospike Cloud.
 * 
 * @author tfaulkes
 *
 */
public class AerospikeConnector {
    private static final String ASDB_CLOUD_DOMAIN = ".asdb.io";

    static class ParseException extends RuntimeException {
        private static final long serialVersionUID = 5652947902453765251L;

        public ParseException(String message) {
            super(message);
        }
    }

    private TlsOptions tls;
    private String hosts = "localhost:3100";
    private String userName;
    private String password;
    private String clusterName;
    private boolean servicesAlternate;
    private AuthMode authMode;

    private SSLOptions parseTlsContext(String tlsContext) {
        SSLOptions options = new SSLOptions();

        StringWithOffset stringData = new StringWithOffset(tlsContext);
        stringData.checkAndConsumeSymbol('{');
        while (!stringData.isSymbol('}', false)) {
            String subkey = stringData.getString();
            stringData.checkAndConsumeSymbol(':');
            String subValue = stringData.getString();
            if (!stringData.isSymbol('}', false)) {
                stringData.checkAndConsumeSymbol(',');
            }
            switch (subkey) {
            case "certChain":
                options.setCertChain(subValue);
                break;
            case "privateKey":
                options.setPrivateKey(subValue);
                break;
            case "caCertChain":
                options.setCaCertChain(subValue);
                break;
            case "keyPassword":
                options.setKeyPassword(subValue);
                break;
            default:
                throw new ParseException("Unexpected key '" + subkey
                        + "' in TLS Context. Valid keys are: 'certChain', 'privateKey', 'caCertChain', and 'keyPassword'");
            }
        }
        return options;
    }

    private void setPropertyOnTlsPolicy(TlsOptions tlsOptions, String key, String value) {
        switch (key) {
        case "protocols":
            tlsOptions.setProtocols(value);
            break;
        case "ciphers":
            tlsOptions.setCiphers(value);
            break;
        case "revokeCerts":
            tlsOptions.setRevokeCertificates(value);
            break;
        case "loginOnly":
            tlsOptions.setLoginOnly(Boolean.parseBoolean(value));
            break;
        case "context":
            tlsOptions.setSsl(parseTlsContext(value));
            break;
        default:
            throw new ParseException("Unexpected key '" + key
                    + "' in TLS policy. Valid keys are: 'protocols', 'ciphers', 'revokeCerts', 'context' and 'loginOnly'");
        }
    }

    /**
     * Parse the TLS string to enable connection to the secure Aerospike cluster.
     * This will include both TLS options and SSL options. For example:
     * 
     * <pre>
     * --tls '{"context":{"certChain":"cert.pem","privateKey":"key.pem","caCertChain":"cacert.pem"}}
     * </pre>
     * 
     * @param tlsOptions
     * @return
     */
    private TlsOptions parseTlsOptions(String tlsOptions) {
        if (tlsOptions != null) {
            TlsOptions policy = new TlsOptions();
            StringWithOffset stringData = new StringWithOffset(tlsOptions);
            if (stringData.isSymbol('{')) {
                while (true) {
                    String key = stringData.getString();
                    if (key != null) {
                        stringData.checkAndConsumeSymbol(':');
                        String value = stringData.getString();
                        setPropertyOnTlsPolicy(policy, key, value);
                    }
                    if (stringData.isSymbol('}')) {
                        break;
                    } else {
                        stringData.checkAndConsumeSymbol(',');
                    }
                }

            }
            return policy;
        }
        return null;
    }

    /**
     * Return the <code>Options</code> object with the parameters pre-specified to
     * allow connection to an Aerospike database. This object can be amended with
     * other user-desired options
     * 
     * @return And <code>Options</code> object
     */
    public Options getOptions() {
        Options options = new Options();
        options.addOption("h", "hosts", true,
                "List of seed hosts for cluster in format: " + "hostname1[:tlsname][:port1],...\n"
                        + "The tlsname is only used when connecting with a secure TLS enabled server. "
                        + "If the port is not specified, the default port is used. "
                        + "IPv6 addresses must be enclosed in square brackets.\n" + "Default: localhost:3000\n"
                        + "Examples:\n" + "host1\n" + "host1:3000,host2:3000\n"
                        + "192.168.1.10:cert1:3000,[2001::1111]:cert2:3000\n");
        options.addOption("U", "user", true, "User name for cluster");
        options.addOption("P", "password", true, "Password for cluster");
        options.addOption("t", "tls", true,
                "Set the TLS Policy options on the cluster. The value passed should be a JSON string. Valid keys in this "
                        + "string inlcude 'protocols', 'ciphers', 'revokeCerts', 'context' and 'loginOnly'. For 'context', the value should be a JSON string which "
                        + "can contain keys 'certChain' (path to the certificate chain PEM), 'privateKey' (path to the certificate private key PEM), "
                        + "'caCertChain' (path to the CA certificate PEM), 'keyPassword' (password used for the certificate chain PEM), 'tlsHost' (the tlsName of the Aerospike host). "
                        + "For example: --tls '{\"context\":{\"certChain\":\"cert.pem\",\"privateKey\":\"key.pem\",\"caCertChain\":\"cacert.pem\"}}'");
        options.addOption("a", "authMode", true, "Set the auth mode of cluster1. Default: INTERNAL");
        options.addOption("cn", "clusterName", true, "Set the cluster name");
        options.addOption("sa", "useServicesAlternate", false,
                "Use services alternative when connecting to the cluster");
        return options;
    }

    /**
     * Validate the passed options as they pertain to connecting to the cluster. If
     * there are validation errors, the first one is returned as a string. If
     * 
     * <pre>
     * null
     * </pre>
     * 
     * is returned the connection parameters are valid.
     * <p/>
     * Note that this method does not attempt to connect to the cluster. Use
     * 
     * <pre>
     * connect()
     * </pre>
     * 
     * for that.
     * 
     * @param cl - the command line containing the passed options
     * @return
     */
    public String validateConnectionsOptions(CommandLine cl) {
        this.hosts = cl.getOptionValue("hosts", "localhost:3000");
        this.userName = cl.getOptionValue("user");
        this.password = cl.getOptionValue("password");
        this.tls = parseTlsOptions(cl.getOptionValue("tls"));
        this.authMode = AuthMode.valueOf(cl.getOptionValue("authMode", "INTERNAL").toUpperCase());
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
     * Return the TlsPolicy as a String
     * 
     * @param policy
     * @return
     */
    private String tlsPolicyAsString(TlsPolicy policy) {
        if (policy == null) {
            return "null";
        } else {
            return String.format("protocols: %s, ciphers: %s, revokeCertificates: %s, forLoginOnly: %b",
                    policy.protocols == null ? "null" : "[" + String.join(",", policy.protocols) + "]",
                    policy.ciphers == null ? "null" : "[" + String.join(",", policy.ciphers) + "]",
                    policy.revokeCertificates == null ? "null" : "[" + policy.revokeCertificates.length + " items]",
                    policy.forLoginOnly);
        }
    }

    /**
     * Connect to the Aerospike cluster using options specified on the command line.
     * 
     * @return A connection to the Aerospike database
     */
    public IAerospikeClient connect() {
        return connect(new ClientPolicy());
    }

    /**
     * Connect to the Aerospike cluster using the command line options. The passed
     * 
     * <pre>
     * ClientPolicy
     * </pre>
     * 
     * will be used to connect with the connection details (hosts, username, etc)
     * overridden by the command line. This is useful if the various
     * 
     * <pre>
     * defaultXXXPolicy
     * </pre>
     * 
     * need to be overridden.
     * 
     * @param policy
     * @return A connection to the Aerospike database
     */
    public IAerospikeClient connect(ClientPolicy clientPolicy) {
        clientPolicy.user = this.getUserName();
        clientPolicy.password = this.getPassword();
        clientPolicy.tlsPolicy = this.getTls() == null ? null : this.getTls().toTlsPolicy();
        clientPolicy.authMode = this.getAuthMode();
        clientPolicy.clusterName = this.getClusterName();
        clientPolicy.useServicesAlternate = this.isServicesAlternate();
        Host[] hosts = Host.parseHosts(this.hosts, 3000);
        IAerospikeClient client = new AerospikeClient(clientPolicy, hosts);

        Log.info(
                String.format(
                        "Cluster: name: %s, hosts: %s user: %s, password: %s\n"
                                + "         authMode: %s, tlsPolicy: %s\n",
                        clientPolicy.clusterName, Arrays.toString(hosts), clientPolicy.user,
                        clientPolicy.password == null ? "null" : "********", clientPolicy.authMode,
                        tlsPolicyAsString(clientPolicy.tlsPolicy)));
        return client;
    }

    public TlsOptions getTls() {
        return tls;
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

    public AuthMode getAuthMode() {
        return authMode;
    }
}
