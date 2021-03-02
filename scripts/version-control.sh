set -e
TYPE=${1}

VERSION=$(cat $PWD/version)

V1=$(echo $VERSION | cut -d'.' -f1)
V2=$(echo $VERSION | cut -d'.' -f2)
V3=$(echo $VERSION | cut -d'.' -f3)

if [[ $TYPE == "patch" ]]; then
    V3=$(($V3 + 1))
elif [[ $TYPE == "minor" ]]; then
    V3=0
    V2=$(($V2 + 1))
elif [[ $TYPE == "major" ]]; then
    V3=0
    V2=0
    V1=$(($V1 + 1))
else
    echo "version type must be patch, minor or major"
    exit 1
fi;

NEW_VERSION=$V1.$V2.$V3
echo $NEW_VERSION > './version'

