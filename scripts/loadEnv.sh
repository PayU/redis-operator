set -e
[[ $ENV ]] && echo Environment is $ENV

function setCommonEnvVars() {
    export TAGS=$(git tag -l --sort=-v:refname)
    export LATEST_TAG=$(echo $TAGS | head -n1 | cut -d " " -f1)
    export LATEST_TAG="${LATEST_TAG%-internal}" # remove internal suffix if exists
    if [ -z ${CI_COMMIT_TAG+x} ] && [[ ! $CI_COMMIT_REF_NAME == cicd* ]]; then
        echo "Docker image will set on branches folder in registry"
        export REL_CI_REGISTRY_IMAGE=$CI_REGISTRY_IMAGE/branches
        export DOCKER_IMAGE=$REL_CI_REGISTRY_IMAGE:$CI_COMMIT_REF_NAME
    else
        echo "Docker image will set on cicd folder in registry (which is the release folder)"
        export REL_CI_REGISTRY_IMAGE=$CI_REGISTRY_IMAGE/cicd
        export DOCKER_IMAGE=$REL_CI_REGISTRY_IMAGE:$LATEST_TAG
    fi    
}

function reportEnvVars() {
    echo "DOCKER_IMAGE: $DOCKER_IMAGE"
    echo "LATEST_TAG: $LATEST_TAG"
}

setCommonEnvVars
reportEnvVars

