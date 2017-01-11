while IFS='' read -r line || [[ -n "$line" ]]; do
    echo "$line"
    sleep 0.1s
done < "$1"
