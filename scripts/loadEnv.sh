set -e
[[ $ENV ]] && echo Environment is $ENV

function setCommonEnvVars() {
    export VERSION=$(git describe --tags --abbrev=0)
    if [ -z ${CI_COMMIT_TAG+x} ] && [[ ! $CI_COMMIT_REF_NAME == cicd* ]]; then
        echo "Docker image will set on branches folder in registry"
        export REL_CI_REGISTRY_IMAGE=$CI_REGISTRY_IMAGE/branches
        export DOCKER_IMAGE=$REL_CI_REGISTRY_IMAGE:$CI_COMMIT_REF_NAME
    else
        echo "Docker image will set on cicd folder in registry (which is the release folder)"
        export REL_CI_REGISTRY_IMAGE=$CI_REGISTRY_IMAGE/cicd
        export DOCKER_IMAGE=$REL_CI_REGISTRY_IMAGE:$VERSION
    fi    
}

function reportEnvVars() {
    echo "DOCKER_IMAGE: $DOCKER_IMAGE"
    echo "VERSION: $VERSION"
}

setCommonEnvVars
reportEnvVars

