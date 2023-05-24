trap 'exit 1' INT


echo "Running test $1 for $2 iters"
for i in $(seq 1 $2); do
    #echo -ne "\r$i / $2"
    rm Id*
    echo $i / $2
    #LOG="$1_$i.txt"
    # Failed go test return nonzero exit codes
    go test -run $1 $3
    #&> $LOG
    #if [[ $? -eq 0 ]]; then
    #    rm $LOG
    #else
    #    echo "Failed at iter $i, saving log at $LOG"
        
    #fi
     if [[ $? -eq 1 ]]; then
        echo "Failed at iter $i"
        exit 1
    fi
done
