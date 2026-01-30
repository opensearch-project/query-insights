#!/bin/bash

set -e

function usage() {
    echo ""
    echo "This script is used to run integration tests for plugin installed on a remote OpenSearch/Dashboards cluster."
    echo "--------------------------------------------------------------------------"
    echo "Usage: $0 [args]"
    echo ""
    echo "Optional arguments:"
    echo -e "-s SECURITY_ENABLED\t(true | false), defaults to true. Specify the OpenSearch/Dashboards have security enabled or not."
    echo -e "-c CREDENTIAL\t(usename:password), no defaults, effective when SECURITY_ENABLED=true."
    echo -e "-h Print this message."
    echo -e "-v OPENSEARCH_VERSION\t, no defaults"
    echo -e "-n SNAPSHOT\t\t, defaults to false"
    echo "Required arguments:"
    echo "Single cluster test:"
    echo -e "-b BIND_ADDRESS\t\t, IP or domain name for the cluster location."
    echo -e "-p BIND_PORT\t\t, port for the cluster location."
    echo -e "-t TRANSPORT_PORT\t, defaults to 9300, can be changed to any port for the cluster location."
    echo "--------------------------------------------------------------------------"
    echo "Multi cluster test:"
    echo -e "-e Comma seperated endpoint:port, ex: localhost:9200:9300,localhost:9201:9301... ."
    echo "--------------------------------------------------------------------------"
}

while getopts ":h:b:p:t:e:s:c:v:" arg; do
    case $arg in
        h)
            usage
            exit 1
            ;;
         b)
            BIND_ADDRESS=$OPTARG
            ;;
         p)
            BIND_PORT=$OPTARG
            ;;
         t)
            TRANSPORT_PORT=$OPTARG
            ;;
        e)
            ENDPOINT_LIST=$OPTARG
            ;;
        s)
            SECURITY_ENABLED=$OPTARG
            ;;
        c)
            CREDENTIAL=$OPTARG
            ;;
        v)
            OPENSEARCH_VERSION=$OPTARG
            ;;
        :)
            echo "-${OPTARG} requires an argument"
            usage
            exit 1
            ;;
        ?)
            echo "Invalid option: -${OPTARG}"
            exit 1
            ;;
    esac
done

# Common starts
if [ -z "$SECURITY_ENABLED" ]
then
  SECURITY_ENABLED="true"
fi

OPENSEARCH_REQUIRED_VERSION="2.12.0"

if [ -z "$CREDENTIAL" ]
then
  # Starting in 2.12.0, security demo configuration script requires an initial admin password
  # Pick the minimum of two versions
  VERSION_TO_COMPARE=`echo $OPENSEARCH_REQUIRED_VERSION $OPENSEARCH_VERSION | tr ' ' '\n' | sort -V | uniq | head -n 1`
  # Check if the compared version is not equal to the required version.
  # If it is not equal, it means the current version is older.
  if [ "$VERSION_TO_COMPARE" != "$OPENSEARCH_REQUIRED_VERSION" ]; then
    CREDENTIAL="admin:admin"
  else
    CREDENTIAL="admin:myStrongPassword123!"
  fi
fi

USERNAME=`echo $CREDENTIAL | awk -F ':' '{print $1}'`
PASSWORD=`echo $CREDENTIAL | awk -F ':' '{print $2}'`
# Common ends


# Check if test is run on multiple cluster

if [ -z "$ENDPOINT_LIST" ]
then
  echo "requires an argument -e <endpoint:port>"
  usage
  exit 1
fi

cluster_name=`echo $ENDPOINT_LIST | jq -r '.[].cluster_name'`
cluster_node1_endpoint=`echo $ENDPOINT_LIST | jq -r '.[].data_nodes[0].endpoint'`
cluster_node1_port=`echo $ENDPOINT_LIST | jq -r '.[].data_nodes[0].port'`
cluster_node_num=`echo $ENDPOINT_LIST | jq -r '.[].data_nodes | length'`

echo "cluster_name: $cluster_name, cluster_node1_endpoint: $cluster_node1_endpoint, cluster_node0_port: $cluster_node0_port, cluster_node_num: $cluster_node_num"

set -x
./gradlew --no-daemon integTestRemote -Dtests.rest.cluster=$cluster_node1_endpoint:$cluster_node1_port -Dtests.cluster=$cluster_node1_endpoint:$cluster_node1_port -Dtests.clustername=$cluster_name -Dhttps=$SECURITY_ENABLED -Duser=$USERNAME -Dpassword=$PASSWORD -PnumNodes=$cluster_node_num --console=plain
