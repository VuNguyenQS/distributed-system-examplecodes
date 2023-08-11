echo "Running test $1 with $2 times"
for i in $(seq 1 $2)
do 
    echo "Start $i/$2"
    rm logInfo/*
    go test -run $1
    if [[ $? -eq 1 ]]; then
        echo "Failed at iter $i"
        exit 1
    fi
done