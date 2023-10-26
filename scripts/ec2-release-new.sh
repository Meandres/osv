#!/usr/bin/env bash
set -u

# check prerequisits
command -v aws &>/dev/null || (echo "Need aws cli installed (apt install awscli)"; exit)
# parse cli flags
PARAM_IMAGE="--image"
PARAM_BUCKET="--bucket"
PARAM_REGION="--region"
PARAM_BLOCK_DEVICE="--block-device"
PARAM_S3_URI="--s3-uri"
PARAM_HELP="--help"

DEFAULT_BUCKET="osv-forge"
BUCKET=$DEFAULT_BUCKET
DEFAULT_REGION="us-east-1"
REGION=$DEFAULT_REGION
DEFAULT_IMAGE="build/last/usr.img"
IMAGE=$DEFAULT_IMAGE

DEFAULT_BLOCK_DEVICE="/dev/sda1"
BLOCK_DEVICE=$DEFAULT_BLOCK_DEVICE

S3_URI=""

print_help() {
 cat <<HLPEND

Create an AMI from a pre-built image and upload it to EC2.

This script assumes following packages are installed and functional:

    1. qemu-img utility
       apt install qemu-utils

    2. AWS Command Line Interface (http://aws.amazon.com/cli/)
       apt install awscli

    3. jq json parser
       apt install jq

This script requires a valid AWS credential setup on the machine it is running on.
Usually this means either running on an EC2 instance with acceptable permissions,
or having a '~/.aws/credentials' file.

This script receives following command line arguments:

    $PARAM_HELP - print this help screen and exit
    $PARAM_IMAGE <image file> - do not rebuild OSv, upload specified image instead (default $DEFAULT_IMAGE)
    $PARAM_BUCKET <bucket name> - the bucket name to upload the image to for import (default $DEFAULT_BUCKET)
    $PARAM_REGION <region> - AWS region to work in (default $DEFAULT_REGION)
    $PARAM_BLOCK_DEVICE <region> - Block device to mount the image under (default $DEFAULT_BLOCK_DEVICE)
    $PARAM_S3_URI <s3-uri> - Don't upload a local image to S3, use an image on S3 instead (default $S3_URI)

HLPEND
}

while test "$#" -ne 0
do
  case "$1" in
    "$PARAM_IMAGE")
      IMAGE=$2
      shift 2
      ;;
    "$PARAM_REGION")
      REGION=$2
      shift 2
      ;;
    "$PARAM_BUCKET")
      BUCKET=$2
      shift 2
      ;;
    "$PARAM_BLOCK_DEVICE")
      BLOCK_DEVICE=$2
      shift 2
      ;;
    "$PARAM_S3_URI")
      S3_URI=$2
      shift 2
      ;;
    "$PARAM_HELP")
      print_help
      exit 0
      ;;
    *)
      shift
      ;;
    esac
done

cat <<HLPEND
Parsed Flags: $PARAM_IMAGE $IMAGE $PARAM_REGION $REGION $PARAM_BUCKET $BUCKET $PARAM_BLOCK_DEVICE $BLOCK_DEVICE
HLPEND

handle_error() {
 echo $(tput setaf 1)">>> [`timestamp`] ERROR: $*"$(tput sgr0)
 echo
 amend_rstatus ERROR: $*
 amend_rstatus Release FAILED.
}

echo_progress() {
 echo $(tput setaf 3)">>> $*"$(tput sgr0)
}

amend_rstatus() {
 echo \[`timestamp`\] $* >> $OSV_RSTATUS
}

get_image_type() {
    qemu-img info $1 | grep "file format" | awk '{print $NF}'
}

get_json_value() {
 #python -c "import json,sys;obj=json.load(sys.stdin);print obj$*"
 jq $*
}

ec2_response_value() {
 local ROWID=$1
 local KEY=$2
 grep $ROWID | sed -n "s/^.*$KEY\s\+\(\S\+\).*$/\1/p"
}

while test x"$S3_URI" == x""; do
    ## if test x"$AWS_ACCESS_KEY_ID" = x"" || test x"$AWS_SECRET_ACCESS_KEY" = x""; then
    ##     handle_error No AWS credentials found. Please define AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY.
    ##     break
    ## fi

    IMAGE_FORMAT=`get_image_type $IMAGE`
    if test x"$IMAGE_FORMAT" != x"raw"; then
        echo_progress "Given image is not in RAW format."
        IMG_OUTPUT=`dirname $IMAGE`/osv.raw
        if test -f $IMG_OUTPUT; then
            echo_progress "File with raw extension in same directory exists, using that."
        else
            echo_progress "Given image is not in RAW Format, converting using scripts/convert"
            qemu-img convert -O raw $IMAGE $IMG_OUTPUT
            if test x"$?" != x"0"; then
                handle_error Failed to convert image to RAW format, please do so yourself
                break
            fi
        fi
        IMAGE=$IMG_OUTPUT
    fi

    S3_UID=osv-$USER-at-`hostname`-`date -u +'%Y-%m-%dT%H-%M-%SZ'`.raw
    S3_URI=s3://$BUCKET/$S3_UID
    echo_progress "Uploading image to S3 at $S3_URI. Assuming the bucket exists."

    aws s3 cp $IMAGE $S3_URI
    if test x"$?" != x"0"; then
        handle_error Failed to upload $IMAGE to $S3_URI
        break
    fi

    break
done

while test x"$S3_URI" != x""; do
    test -z "${S3_UID+x}" && S3_UID=`basename $S3_URI`
    DESCRIPTION="$S3_UID"

    echo_progress "Starting snapshot import task from S3 to EC2."
    TASK_ID=`aws ec2 import-snapshot \
        --description "$DESCRIPTION" \
        --region "$REGION" \
        --disk-container "Format=raw,UserBucket={S3Bucket=$BUCKET,S3Key=$S3_UID}" \
        | tee /dev/tty | jq -r '.ImportTaskId'`

    if test x"$?" != x"0"; then
        handle_error Failed to import snapshot, stopping.
        break
    fi

    echo_progress "Waiting for snapshot to finish, logging progress..."

    STATUS=""

    while test x"$STATUS" != x"completed"; do
        STATUS=`aws ec2 describe-import-snapshot-tasks --import-task-ids $TASK_ID --region $REGION \
            | tee /dev/tty | jq -r '.ImportSnapshotTasks[0].SnapshotTaskDetail.Status'`
        sleep 2
    done

    SNAPSHOT=`aws ec2 describe-import-snapshot-tasks --import-task-ids $TASK_ID --region $REGION \
        | tee /dev/tty | get_json_value -r '.ImportSnapshotTasks[0].SnapshotTaskDetail.SnapshotId'`

    echo_progress "Creating image from snapshot $SNAPSHOT"

    aws ec2 register-image \
        --block-device-mappings "DeviceName=$BLOCK_DEVICE,Ebs={SnapshotId=$SNAPSHOT}" \
        --root-device-name $BLOCK_DEVICE \
        --ena-support \
        --region $REGION \
        --virtualization-type hvm \
        --name $S3_UID

    if test x"$?" != x"0"; then
        handle_error "Failed to convert snapshot to image. Do it manually in the AWS UI"
        break
    fi

    break
done
