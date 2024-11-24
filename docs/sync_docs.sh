#!/bin/bash

# get git hash for commit message
GITHASH=$(git rev-parse HEAD)
MSG="doc build for commit $GITHASH"
cd build

# clone the repo if needed
if test -d vegafusion.github.io;
then rm -rf vegafusion.github.io;
fi

git clone git@github.com:vegafusion/vegafusion.github.io.git;

# sync the website
cd vegafusion.github.io
git pull

# switch to gh-pages branch
git checkout gh-pages

remove all tracked files
git ls-files -z | xargs -0 rm -f

# sync files from html build
rsync -r ../html/ ./

# add commit, and push to github
git add . --all
git commit -m "$MSG"

# Add confirmation prompt before pushing
read -p "Ready to push to gh-pages branch. Continue? (y/n) " answer
if [[ $answer == "y" || $answer == "Y" ]]; then
    git push origin gh-pages
else
    echo "Push cancelled"
    exit 1
fi
