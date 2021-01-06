set -e
[[ $ENV ]] && echo Environment is $ENV

function setCommonEnvVars() {
    if [ -z ${CI_COMMIT_TAG+x} ] && [[ ! $CI_COMMIT_REF_NAME == master* ]]; then
        echo "Docker image will set on branches folder in registry"
        export REL_CI_REGISTRY_IMAGE=$CI_REGISTRY_IMAGE/branches
    elif [ -z ${CI_COMMIT_TAG+x} ]; then
        echo "Docker image will set on master folder in registry"
        export REL_CI_REGISTRY_IMAGE=$CI_REGISTRY_IMAGE/master
    else
        echo "Docker image will set on releases folder in registry"
        export REL_CI_REGISTRY_IMAGE=$CI_REGISTRY_IMAGE/releases
    fi

    export DOCKER_IMAGE=$REL_CI_REGISTRY_IMAGE:$CI_COMMIT_REF_NAME
    export VERSION=$(cat $PWD/version)
}

function reportEnvVars() {
    echo "DOCKER_IMAGE: $DOCKER_IMAGE"
    echo "VERSION: $VERSION"
}

setCommonEnvVars
reportEnvVars

