#! /usr/bin/env bash
# Stage dependencies required to build dss-minio.
# MinIO relies on dss-sdk NKV headers and libs, compiled from the dss-sdk repo.
# This script downloads the latest compiled 'main' dss-sdk artifact and stages them for the MinIO build.
# Additionally, s3-benchmark is downloaded to test client functionality.
# This script is intended for CI/CD use only.
set -e

# Path vars
DSSSDKGLOB='nkv-sdk-bin-*.tgz'
S3BENCHURL='https://github.com/OpenMPDK/dss-ecosystem/raw/master/dss_s3benchmark/s3-benchmark'
S3BENCHPATH='dss-ecosystem/dss_s3benchmark/s3-benchmark'

# Check if DSSS3URI is defined
if [[ "$DSSS3URI" == '' ]]
then
    echo "*** ERROR: DSSS3URI var not defined"
    exit 1
fi

# Check if GITHUB_REF_NAME or CI_PIPELINE_SOURCE is defined
if [[ "$GITHUB_REF_NAME" == '' &&  "$CI_PIPELINE_SOURCE" == '' ]]
then
    echo "*** ERROR: GITHUB_REF_NAME or CI_PIPELINE_SOURCE var not defined"
    exit 1
fi

# Derive the branch name for corresponding dss-sdk artifact
# Use GITHUB_BASE_REF if GitHub PR
if [[ "$GITHUB_REF_NAME" == *"/merge" ]]
then
    BRANCH_NAME=$GITHUB_BASE_REF
# Use GITHUB_REF_NAME if GitHub merge or push
elif [[ "$GITHUB_REF_NAME" ]]
then
    BRANCH_NAME=$GITHUB_REF_NAME
# Use CI_MERGE_REQUEST_TARGET_BRANCH_NAME if GitLab MR
elif [[ "$CI_PIPELINE_SOURCE" == "merge_request_event" ]]
then
    BRANCH_NAME=$CI_MERGE_REQUEST_TARGET_BRANCH_NAME
# Use CI_COMMIT_BRANCH if GitLab merge or push
elif [[ "$CI_COMMIT_BRANCH" ]]
then
    BRANCH_NAME=$CI_COMMIT_BRANCH
else
    echo "*** ERROR: Could not derive the branch name"
    exit 1
fi

# Get latest dss-sdk main artifact
set +e
DSSSDKARTIFACT=$(aws s3 ${MINIO_HOST_URL:+--endpoint-url $MINIO_HOST_URL} ls "$DSSS3URI/$BRANCH_NAME"/ | sort --reverse | grep -oP "${DSSSDKGLOB//\*/.*}" | head -n 1)
set -e

# Check if dss-sdk artifact found in bucket
if [[ "$DSSSDKARTIFACT" == '' ]]
then
    echo "*** ERROR: I couldn't find any existing artifacts matching glob '$DSSSDKGLOB' in the bucket for branch '$BRANCH_NAME'."
    exit 1
fi

# Download and extract dss-sdk artifact
echo "Staging dss-sdk libs and includes from artifact: $DSSSDKARTIFACT from branch '$BRANCH_NAME'"
mkdir -p ../dss-sdk/host ../dss-sdk/host_out
aws s3 ${MINIO_HOST_URL:+--endpoint-url $MINIO_HOST_URL} cp "$DSSS3URI/$BRANCH_NAME/$DSSSDKARTIFACT" - | tar xfz - --wildcards --directory=../dss-sdk/host/ nkv-sdk/include/* --directory=../host_out/ nkv-sdk/lib/* --strip=1

# Download s3-benchmark
echo "Staging s3-benchmark from URL: $S3BENCHURL"
curl --silent --show-error --create-dirs --output "../$S3BENCHPATH" --location --remote-name "$S3BENCHURL"
chmod +x "../$S3BENCHPATH"
