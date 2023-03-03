#!/usr/bin/env bash
#
# Build docker image with tag based on git revision or tag if it exists
# and push it to the registry. The script is called from .jenkins.yaml.
#
# We also pass on this version string as a --build-arg to the docker
# build so that the resulting binary in the container uses that same
# version string for logs etc.
#
# When modifiying this script run it through shellcheck
# (https://www.shellcheck.net/) before commiting.
#

set -e

script_name=$(basename "$0")
project_name=$(basename "$(pwd)")
project_name_short=$(basename "$(pwd)" | cut -d "-" -f2)

echo "running SUNET/${project_name}/${script_name}"

# We expect Jenkins to have set GIT_COMMIT for us.
if [ "$GIT_COMMIT" = "" ]; then
    echo "$script_name: GIT_COMMIT is not set, exiting"
    exit 1
fi

VERSION=$(git tag --contains "${GIT_COMMIT}" | head -1)
if [ "$VERSION" = "" ]; then
    echo "${script_name}: did not find a tag related to revision ${GIT_COMMIT}, using rev as version"
    VERSION=${GIT_COMMIT}
fi

BASE_TAG="docker.sunet.se/${project_name_short}"
DOCKER_TAG="${BASE_TAG}:${VERSION}"
LATEST_TAG="${BASE_TAG}:latest"
echo "${script_name}: building DOCKER_TAG ${DOCKER_TAG} ${LATEST_TAG}"

docker build --tag "${DOCKER_TAG}" --tag "${LATEST_TAG}" .
#docker push --all-tags ${BASE_TAG}
# Workaround for old docker verison on CI
for tag in ${DOCKER_TAG} ${LATEST_TAG}; do
    docker push "${tag}"
done
