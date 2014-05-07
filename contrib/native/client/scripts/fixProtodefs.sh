#!/bin/bash
SRCDIR=$1  #../../../../protocol/src/main/protobuf
TARGDIR=$2 #../build/protobuf
FNAME=$3   #Types.proto

fixFile(){
    echo "Fixing $1"
    pushd ${TARGDIR} >& /dev/null
    sed  -e 's/REQUIRED/DM_REQUIRED/g' -e  's/OPTIONAL/DM_OPTIONAL/g' -e 's/REPEATED/DM_REPEATED/g' -e 's/NULL/DM_UNKNOWN/g' $1 > temp1.proto
    cp temp1.proto $1
    rm temp1.proto
    popd >& /dev/null
}

main() {
    if [ ! -e ${TARGDIR} ]
    then
        echo "Creating Protobuf directory"
        mkdir -p ${TARGDIR}
        cp -r ${SRCDIR}/* ${TARGDIR}
        fixFile ${FNAME}
    else
        cp -r ${SRCDIR}/* ${TARGDIR}

        if [ -e ${TARGDIR}/${FNAME} ]
        then
            if [ ${SRCDIR}/${FNAME} -nt ${TARGDIR}/${FNAME} ] 
            then
                fixFile ${FNAME}
            fi
        else
            echo "$FNAME not found"
            exit 1
        fi
    fi
}

main

