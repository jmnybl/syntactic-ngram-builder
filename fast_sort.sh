# These are needed so that sort/uniq uses a simple byte comparison and nothing language-specific, same is needed for sort
export LC_ALL=C
export LC_COLLATE=C

# to drop singleton ngrams, use uniq -d
# if you have a SSD drive, it makes sense to use "-T path/to/ssd" in sort 

for f in $1/*
do
    echo $f
    pigz -d -c $f -p 4 | sort -S 50G --parallel 20 --compress-program "./pigz.sh" | uniq -c | perl -pe 's/^\s*([0-9]+)\s+(.*)$/\2\t\1/' | sort -S 50G --parallel 20 -n -r -k 3 --field-separator=$'\t' --compress-program "./pigz.sh" | pigz -p 20 -b 2048 -c > ${f%.txt.gz}.sorted_by_count.txt.gz
done
