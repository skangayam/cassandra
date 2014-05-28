#!/bin/sh

JABBA_RELEASE_TYPE=$1
if [ "$JABBA_RELEASE_TYPE" == "hotfix" ]
then
  version=`date -u "+%Y%m%d%H%M%S"`-HOTFIX

  # Save the existing value of the develop branch
  export JABBA_DEVELOP_BRANCH_OLD=$(awk '/develop =/' $JABBA_HOME/.git/config | awk '{print $3}')
  export JABBA_DEVELOP_BRANCH_NEW=$2
    if [[ -z "$JABBA_DEVELOP_BRANCH_NEW" ]]
    then
      echo "ERROR: Hotfix branch not provided!"
      exit 1
    fi

  # Replace the develop branch's value with the new hotfix branch provided
  sed -i .bak "s|develop = ${JABBA_DEVELOP_BRANCH_OLD}|develop = ${JABBA_DEVELOP_BRANCH_NEW}|g" $JABBA_HOME/.git/config
else
  version=`date -u "+%Y%m%d%H%M%S"`
  # A successful completion of the post-flow-release-finish unset's these variables too.
  unset JABBA_DEVELOP_BRANCH_OLD
  unset JABBA_DEVELOP_BRANCH_NEW
fi

echo "milestone version: $version"

cp ./build/git/hooks/* $JABBA_HOME/.git/hooks

git flow release start -F $version
git flow release finish -m "milestone: $version" -p -D $version
