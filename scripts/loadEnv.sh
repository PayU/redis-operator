set -e
[[ $ENV ]] && echo Environment is $ENV

function setCommonEnvVars() {
    setLatestTag

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

function setLatestTag() {
  export TAGS=$(git tag -l --sort=-v:refname)
  export LATEST_TAG=$(echo $TAGS | head -n1 | cut -d " " -f1)

  if [[ $LATEST_TAG == *internal* ]]; then
      export INTERNAL_TAG_NUMBER=${LATEST_TAG##*-}
      if [ "$INTERNAL_TAG_NUMBER" == "internal" ]; then
        echo "INTERNAL_TAG_NUMBER was identified as the 'internal' string. setting it to 1"
        export INTERNAL_TAG_NUMBER=1
      else
        # means this is just an internal release, we need to increase the internal tag number by 1
        echo "increasing internal tag number by 1"
        INTERNAL_TAG_NUMBER=$((INTERNAL_TAG_NUMBER+1))
      fi
  else
    echo "new open source release tag was identified. setting INTERNAL_TAG_NUMBER to 1"
    export INTERNAL_TAG_NUMBER=1
  fi

  export LATEST_TAG="${LATEST_TAG%-internal*}" # remove internal suffix if exists
}

function reportEnvVars() {
    echo "DOCKER_IMAGE: $DOCKER_IMAGE"
    echo "LATEST_TAG: $LATEST_TAG"
    echo "INTERNAL_TAG_NUMBER: $INTERNAL_TAG_NUMBER"
}

setCommonEnvVars
reportEnvVars

